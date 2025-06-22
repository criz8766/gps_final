# eventos/app/kafka_consumer.py
import os
import json
import time
import threading
import logging # <--- 1. Importar logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from app.mongo_handler import guardar_evento_en_mongo
from dotenv import load_dotenv

# 2. Configurar un logger básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_PACIENTES", "pacientes-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_EVENTOS", "eventos-pacientes-group")
RETRY_DELAY_SECONDS = 10
_shutdown_event = None 

def inicializar_consumidor_kafka():
    consumer = None
    max_retries = 5
    for attempt in range(max_retries):
        if _shutdown_event and _shutdown_event.is_set():
            logger.info("Señal de apagado recibida durante inicialización del consumidor.")
            return None
        try:
            logger.info(f"(Intento {attempt + 1}/{max_retries}) Conectando consumidor Kafka a {KAFKA_BOOTSTRAP_SERVERS}...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                auto_offset_reset='earliest',
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            )
            logger.info(f"Consumidor Kafka conectado y suscrito al topic '{KAFKA_TOPIC}' con group_id '{KAFKA_GROUP_ID}'.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"(Intento {attempt + 1}/{max_retries}) No hay brokers de Kafka. Reintentando en {RETRY_DELAY_SECONDS}s...")
            if attempt + 1 < max_retries: time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"(Intento {attempt + 1}/{max_retries}) Error inesperado al conectar consumidor Kafka: {e}")
            if attempt + 1 < max_retries: time.sleep(RETRY_DELAY_SECONDS)

    logger.error(f"No se pudo conectar al broker de Kafka después de {max_retries} intentos.")
    return None

def escuchar_eventos_kafka(shutdown_event_param: threading.Event):
    global _shutdown_event
    _shutdown_event = shutdown_event_param
    consumer = inicializar_consumidor_kafka()

    if not consumer:
        logger.error("No se pudo inicializar el consumidor de Kafka. El hilo terminará.")
        return

    logger.info(f"Escuchando eventos en el topic '{KAFKA_TOPIC}'...")
    try:
        while not _shutdown_event.is_set():
            for message in consumer: # Este iterador bloqueará hasta que haya mensajes o se cierre
                if _shutdown_event.is_set():
                    logger.info("Señal de apagado recibida dentro del bucle de mensajes.")
                    break 
                evento_data = message.value 
                logger.info(f"Evento recibido de Kafka: offset={message.offset}, key={message.key}, value={evento_data}")

                id_insertado = guardar_evento_en_mongo(evento_data)
                if id_insertado:
                    logger.info(f"Evento procesado y guardado en MongoDB con _id: {id_insertado}")
                else:
                    logger.warning(f"El evento no se pudo guardar en MongoDB. Datos: {evento_data}")

            if _shutdown_event.is_set(): # Re-chequear después del bucle por si se cerró desde fuera
                break
            # Pequeña pausa para ceder el control si el bucle del consumidor no bloquea como se espera
            # o si se procesan muchos mensajes muy rápido y queremos ver el shutdown_event.
            # time.sleep(0.1) 

    except KeyboardInterrupt:
        logger.info("Consumidor Kafka detenido por el usuario (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"Error inesperado en el bucle del consumidor Kafka: {e}", exc_info=True) # exc_info para traceback
    finally:
        if consumer:
            logger.info("Cerrando consumidor Kafka...")
            consumer.close()
            logger.info("Consumidor Kafka cerrado.")