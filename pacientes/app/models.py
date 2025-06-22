from pydantic import BaseModel

class PacienteCreate(BaseModel):
    nombre: str
    rut: str
    fecha_nacimiento: str

class Paciente(PacienteCreate):
    id: int

