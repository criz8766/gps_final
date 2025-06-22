// frontend/src/api/pacientes.ts
export interface Paciente {
  id: number;
  nombre: string;
  rut: string;
  fecha_nacimiento: string;
}

export interface PacienteCreate { // También usado para datos de actualización
  nombre: string;
  rut: string;
  fecha_nacimiento: string;
}

const BASE_URL = '/api'; 

// Función genérica para manejar las llamadas fetch con token y errores
async function fetchAPI<T>(url: string, token: string, options: RequestInit = {}): Promise<T> {
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`, // Cabecera de autenticación
    ...options.headers,
  };

  const response = await fetch(url, { ...options, headers });

  if (!response.ok) {
    // Para DELETE con 204 (No Content), response.json() fallará.
    if (response.status === 204 && options.method === 'DELETE') {
      return undefined as T; // O un valor que indique éxito para DELETE
    }
    // Intenta parsear el error del backend
    const errorData = await response.json().catch(() => ({ 
      detail: `Error HTTP ${response.status} sin cuerpo JSON detallado.` 
    }));
    console.error(`Error en la API (${url}):`, errorData);
    throw new Error(errorData.detail || `Error ${response.status} al llamar a la API`);
  }
  // Para DELETE con 204, no hay cuerpo JSON que parsear.
  return response.status === 204 ? (undefined as T) : await response.json();
}

// --- Funciones CRUD que usan el token ---
export async function crearPacienteAPI(paciente: PacienteCreate, token: string): Promise<Paciente> {
  return fetchAPI<Paciente>(`${BASE_URL}/pacientes/`, token, {
    method: 'POST',
    body: JSON.stringify(paciente),
  });
}

export async function listarPacientesAPI(token: string): Promise<Paciente[]> {
  return fetchAPI<Paciente[]>(`${BASE_URL}/pacientes/`, token);
}

export async function actualizarPacienteAPI(id: number, pacienteData: PacienteCreate, token: string): Promise<Paciente> {
  return fetchAPI<Paciente>(`${BASE_URL}/pacientes/${id}`, token, {
    method: 'PUT',
    body: JSON.stringify(pacienteData),
  });
}

export async function eliminarPacienteAPI(id: number, token: string): Promise<void> {
  await fetchAPI<void>(`${BASE_URL}/pacientes/${id}`, token, {
    method: 'DELETE',
  });
}

export async function obtenerPacientePorIdAPI(id: number, token: string): Promise<Paciente> {
  return fetchAPI<Paciente>(`${BASE_URL}/pacientes/${id}`, token);
}

export async function obtenerPacientePorRutAPI(rut: string, token: string): Promise<Paciente> {
  return fetchAPI<Paciente>(`${BASE_URL}/pacientes/rut/${rut}`, token);
}