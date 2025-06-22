# eventos/app/mongo_handler.py
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime, timezone
import logging
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/") # Default si no está en .env
DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sga_eventos")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "eventos_pacientes")

_mongo_client = None
_db = None

def get_mongo_db():
    global _mongo_client, _db
    if _mongo_client is None or _db is None:
        try:
            logger.info(f"INFO: Conectando a MongoDB en {MONGO_URI}...")
            _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            # Verificar la conexión
            _mongo_client.admin.command('ping') 
            _db = _mongo_client[DATABASE_NAME]
            logger.info(f"INFO: Conectado a MongoDB. Base de datos: '{DATABASE_NAME}', Colección: '{COLLECTION_NAME}'")
        except ConnectionFailure as e:
            logger.error(f"ERROR: No se pudo conectar a MongoDB: {e}")
            _mongo_client = None
            _db = None
            # Podrías decidir relanzar la excepción o manejarla de otra forma
            # raise ConnectionFailure(f"No se pudo conectar a MongoDB: {e}") from e
        except Exception as e:
            logger.error(f"ERROR: Ocurrió un error inesperado al conectar con MongoDB: {e}")
            _mongo_client = None
            _db = None
            # raise
    return _db

def guardar_evento_en_mongo(evento_data: dict):
    db = get_mongo_db()
    if db is not None:
        try:
            collection = db[COLLECTION_NAME]
            # Añadir un timestamp de cuándo se procesó este evento
            evento_con_timestamp = {
                **evento_data, # Desempaqueta el evento original
                "timestamp_procesamiento_utc": datetime.now(timezone.utc).isoformat()
            }
            insert_result = collection.insert_one(evento_con_timestamp)
            logger.info(f"INFO: Evento guardado en MongoDB con id: {insert_result.inserted_id}")
            return insert_result.inserted_id
        except Exception as e:
            logger.error(f"ERROR: No se pudo guardar el evento en MongoDB: {e}")
            return None
    else:
        logger.error("ERROR: No hay conexión a MongoDB para guardar el evento.")
        return None

# Opcional: Función para cerrar la conexión si fuera necesario al apagar la app
def cerrar_conexion_mongo():
    global _mongo_client
    if _mongo_client:
        _mongo_client.close()
        logger.info("INFO: Conexión a MongoDB cerrada.")
