import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 1. Construye la URL de conexión a la base de datos usando las variables de entorno
#    El formato es: "postgresql://usuario:contraseña@host/basededatos"
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "farmacia")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"


# 2. Crea el "motor" de SQLAlchemy, que gestionará las conexiones a la BD
engine = create_engine(DATABASE_URL)

# 3. Crea una fábrica de sesiones para interactuar con la BD de forma transaccional
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Crea la clase Base de la que heredarán todos tus modelos de datos (como el modelo Paciente)
Base = declarative_base()

# (Opcional pero buena práctica) Una función para obtener una sesión de BD en las rutas
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()