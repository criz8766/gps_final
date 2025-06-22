from fastapi import APIRouter, HTTPException, Response, Depends
from typing import List
from sqlalchemy.orm import Session

# Importaciones corregidas y añadidas
from app import models, schemas  # Asumiendo que tus modelos Pydantic están en schemas.py
from app.database import get_db # Importamos la nueva función get_db
from app.kafka_producer import KafkaProducer # Lo mantenemos si lo usas
from app.security import validate_token # Mantenemos la seguridad de Auth0

# Inicializamos Kafka (ajusta si es necesario)
# Esta parte podría moverse a main.py si no está ya allí
# kafka_producer = KafkaProducer() 

router = APIRouter()

# --- NOTA IMPORTANTE ---
# El código asume que tus modelos Pydantic se llaman como en el archivo original:
# - Paciente para respuestas (con ID)
# - PacienteCreate para creación/actualización (sin ID)
# Si los tienes en un archivo schemas.py, asegúrate de que los nombres coincidan.


@router.post("/", response_model=schemas.Paciente, status_code=201)
def crear_paciente(paciente: schemas.PacienteCreate, db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    # Creamos una instancia del modelo de la base de datos a partir de los datos Pydantic
    db_paciente = models.Paciente(**paciente.model_dump())
    db.add(db_paciente)
    db.commit()
    db.refresh(db_paciente) # Refrescamos para obtener el ID asignado por la BD

    # Lógica de Kafka (se mantiene)
    # Aquí puedes usar kafka_producer.send(...)
    
    return db_paciente


@router.get("/", response_model=List[schemas.Paciente])
def listar_pacientes(db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    pacientes = db.query(models.Paciente).order_by(models.Paciente.nombre).all()
    return pacientes


@router.get("/{id_paciente}", response_model=schemas.Paciente)
def obtener_paciente_por_id(id_paciente: int, db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    paciente = db.query(models.Paciente).filter(models.Paciente.id == id_paciente).first()
    if paciente is None:
        raise HTTPException(status_code=404, detail=f"Paciente con ID {id_paciente} no encontrado")
    return paciente


@router.get("/rut/{rut_paciente}", response_model=schemas.Paciente)
def obtener_paciente_por_rut(rut_paciente: str, db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    paciente = db.query(models.Paciente).filter(models.Paciente.rut == rut_paciente).first()
    if paciente is None:
        raise HTTPException(status_code=404, detail=f"Paciente con RUT {rut_paciente} no encontrado")
    return paciente


@router.put("/{id_paciente}", response_model=schemas.Paciente)
def actualizar_paciente(id_paciente: int, paciente_update: schemas.PacienteCreate, db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    db_paciente = db.query(models.Paciente).filter(models.Paciente.id == id_paciente).first()

    if db_paciente is None:
        raise HTTPException(status_code=404, detail=f"Paciente con ID {id_paciente} no encontrado para actualizar")

    # Actualizamos los campos del objeto de la base de datos
    for var, value in vars(paciente_update).items():
        setattr(db_paciente, var, value) if value else None

    db.commit()
    db.refresh(db_paciente)

    # Lógica de Kafka (se mantiene)

    return db_paciente


@router.delete("/{id_paciente}", status_code=204)
def eliminar_paciente(id_paciente: int, db: Session = Depends(get_db), current_user_payload: dict = Depends(validate_token)):
    db_paciente = db.query(models.Paciente).filter(models.Paciente.id == id_paciente).first()

    if db_paciente is None:
        raise HTTPException(status_code=404, detail=f"Paciente con ID {id_paciente} no encontrado para eliminar")
    
    # Lógica de Kafka (se mantiene, se ejecuta ANTES de borrar)

    db.delete(db_paciente)
    db.commit()
    
    return Response(status_code=204)