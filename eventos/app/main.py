# eventos/app/main.py
from fastapi import FastAPI, HTTPException
import threading
import logging
import time
# Ajusta la importación si escuchar_eventos_kafka es el nombre correcto
from app.kafka_consumer import escuchar_eventos_kafka # Ya no necesitamos inicializar_consumidor_kafka aquí
from app.mongo_handler import get_mongo_db, cerrar_conexion_mongo
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

app = FastAPI(title="Microservicio de Eventos", version="1.0.0")

kafka_consumer_thread = None
shutdown_event = threading.Event() # Evento para controlar el apagado del hilo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__) 

def iniciar_proceso_consumidor_kafka_con_control():
    logger.info("INFO: Iniciando hilo del consumidor Kafka con control de apagado...")
    escuchar_eventos_kafka(shutdown_event) # <--- Pasar el evento de apagado
    logger.info("INFO: Hilo del consumidor Kafka (controlado) ha terminado.")

@app.on_event("startup")
async def startup_event():
    global kafka_consumer_thread
    logger.info("INFO: Evento de inicio de FastAPI - Microservicio de Eventos.")
    db = get_mongo_db()
    if db is None:
        logger.error("ERROR CRÍTICO: No se pudo conectar a MongoDB al inicio.")

    if kafka_consumer_thread is None or not kafka_consumer_thread.is_alive():
        shutdown_event.clear() 
        kafka_consumer_thread = threading.Thread(target=iniciar_proceso_consumidor_kafka_con_control, daemon=True)
        kafka_consumer_thread.start()
    else:
        logger.info("INFO: El hilo del consumidor Kafka ya está en ejecución.")

@app.on_event("shutdown")
async def shutdown_event_fastapi():
    logger.info("INFO: Evento de apagado de FastAPI - Microservicio de Eventos.")
    global kafka_consumer_thread

    logger.info("INFO: Señalando al hilo del consumidor Kafka para que termine...")
    shutdown_event.set() # Indicar al hilo que debe terminar

    if kafka_consumer_thread and kafka_consumer_thread.is_alive():
        logger.info("INFO: Esperando que el hilo del consumidor Kafka termine (máx 15s)...")
        kafka_consumer_thread.join(timeout=15) 
        if kafka_consumer_thread.is_alive():
            logger.warning("WARN: El hilo del consumidor Kafka no terminó a tiempo.")
        else:
            logger.info("INFO: Hilo del consumidor Kafka terminado limpiamente.")

    cerrar_conexion_mongo()
    logger.info("INFO: Microservicio de Eventos apagado.")

@app.get("/health", summary="Verifica la salud del servicio")
async def health_check():
    if kafka_consumer_thread and kafka_consumer_thread.is_alive():
        consumer_status = "activo"
    else:
        consumer_status = "inactivo o terminado"
    return {"status": "ok", "message": "Microservicio de Eventos está activo.", "kafka_consumer_thread": consumer_status}
