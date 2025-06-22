# Importaciones existentes
from fastapi import FastAPI
from .routes import pacientes
from .kafka_producer import KafkaProducer
import os

# --- INICIO DE LA MODIFICACIÓN ---
# Importa la Base declarativa y el motor de la base de datos
from .database import Base, engine

# ¡LÍNEA CLAVE!
# Esta instrucción le ordena a SQLAlchemy que verifique la base de datos.
# Si las tablas definidas en tus modelos (como Paciente) no existen, las creará.
# Si ya existen, no hará nada.
Base.metadata.create_all(bind=engine)
# --- FIN DE LA MODIFICACIÓN ---


# Creación de la aplicación FastAPI (esto ya lo tenías)
app = FastAPI()

# Incluir las rutas del CRUD de pacientes (esto ya lo tenías)
app.include_router(pacientes.router)

# Inicializar Kafka Producer (esto ya lo tenías)
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:9092")
producer = KafkaProducer(KAFKA_BROKER_URL)

@app.on_event("startup")
async def startup_event():
    # Conectar el productor de Kafka al iniciar (esto ya lo tenías)
    await producer.connect()


@app.on_event("shutdown")
async def shutdown_event():
    # Desconectar el productor de Kafka al apagar (esto ya lo tenías)
    await producer.disconnect()