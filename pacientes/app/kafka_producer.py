# pacientes/app/kafka_producer.py
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable # Importar error específico si quieres capturarlo
import json
import time
import os

_producer_instance = None
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def get_kafka_producer():
    global _producer_instance
    if _producer_instance is None:
        print(f"INFO: Intentando conectar KafkaProducer a: {KAFKA_BOOTSTRAP_SERVERS}")
        max_retries = 5
        retry_delay = 5 # segundos
        for attempt in range(max_retries):
            try:
                _producer_instance = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                    # Considera añadir un client_id para identificar este productor en los logs de Kafka
                    client_id='pacientes-service-producer',
                    # Tiempos de espera para la conexión pueden ayudar, pero el reintento es más robusto
                    # request_timeout_ms=10000, # Aumentar si es necesario
                    # api_version_auto_timeout_ms=10000 # Para la detección de versión del broker
                )
                print("INFO: KafkaProducer conectado exitosamente.")
                break 
            except NoBrokersAvailable as e: # Captura específica para NoBrokersAvailable
                print(f"WARN: Intento {attempt + 1}/{max_retries} fallido al conectar con Kafka (NoBrokersAvailable): {e}")
                if attempt + 1 == max_retries:
                    print("ERROR: Máximo de reintentos alcanzado. Falló la conexión con Kafka.")
                    _producer_instance = None
                else:
                    print(f"INFO: Reintentando conexión con Kafka en {retry_delay} segundos...")
                    time.sleep(retry_delay)
            except Exception as e: # Captura otros errores de KafkaProducer
                print(f"WARN: Intento {attempt + 1}/{max_retries} fallido al conectar con Kafka (Otro Error): {e}")
                if attempt + 1 == max_retries:
                    print("ERROR: Máximo de reintentos alcanzado. Falló la conexión con Kafka.")
                    _producer_instance = None
                else:
                    print(f"INFO: Reintentando conexión con Kafka en {retry_delay} segundos...")
                    time.sleep(retry_delay)
    
    if _producer_instance is None:
        print("ERROR: KafkaProducer no está inicializado después de los reintentos. Los eventos no se enviarán.")
            
    return _producer_instance

def enviar_evento(topic: str, mensaje: dict):
    producer = get_kafka_producer()
    if producer:
        try:
            print(f"INFO: Enviando evento a Kafka (topic: {topic}): {mensaje}")
            future = producer.send(topic, mensaje)
            # Opcional: Esperar la confirmación. Puede añadir latencia.
            # record_metadata = future.get(timeout=10) 
            # print(f"INFO: Evento enviado a topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
            producer.flush() # Importante para asegurar el envío, especialmente si el script termina pronto
            print("INFO: Evento flusheado a Kafka.")
        except Exception as e:
            print(f"ERROR: Error al enviar evento a Kafka: {e}")
    else:
        print(f"ERROR: No se pudo enviar evento a Kafka porque el productor no está disponible. Mensaje: {mensaje}")