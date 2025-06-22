// frontend/src/components/ListarPacientes.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { useAuth0 } from '@auth0/auth0-react';
import { 
  listarPacientesAPI, 
  actualizarPacienteAPI, 
  eliminarPacienteAPI, 
  Paciente, 
  PacienteCreate 
} from '../api/pacientes';
import FormularioPaciente from './FormularioPaciente';
import './ListarPacientes.css'; // Asegúrate que este archivo CSS exista

const ListarPacientes: React.FC = () => {
  const { getAccessTokenSilently, isAuthenticated, isLoading: isLoadingAuth } = useAuth0();
  const [pacientes, setPacientes] = useState<Paciente[]>([]);
  const [mensaje, setMensaje] = useState('');
  const [cargandoLista, setCargandoLista] = useState(false); // Renombrado para evitar conflicto con isLoadingAuth
  
  const [pacienteAEditar, setPacienteAEditar] = useState<Paciente | null>(null);
  const [mostrarModalEdicion, setMostrarModalEdicion] = useState(false);
  const [formDataEdicion, setFormDataEdicion] = useState<PacienteCreate>({
    nombre: '',
    rut: '',
    fecha_nacimiento: '',
  });
  const [isSubmittingEdicion, setIsSubmittingEdicion] = useState(false);

  const cargarPacientes = useCallback(async () => {
    if (!isAuthenticated) return; // No cargar si no está autenticado

    setCargandoLista(true);
    setMensaje('Cargando lista de pacientes...');
    try {
      const token = await getAccessTokenSilently({
        authorizationParams: { audience: process.env.REACT_APP_AUTH0_API_AUDIENCE! },
      });
      const data = await listarPacientesAPI(token);
      setPacientes(data);
      setMensaje(''); // Limpiar mensaje si la carga es exitosa
    } catch (error: any) {
      console.error(error);
      setMensaje(error.message || 'Error desconocido al cargar pacientes');
      setPacientes([]);
    } finally {
      setCargandoLista(false);
    }
  }, [isAuthenticated, getAccessTokenSilently]);

  useEffect(() => {
    if (isAuthenticated) { // Solo cargar pacientes si el usuario está autenticado
      cargarPacientes();
    } else {
      setPacientes([]); // Limpiar lista si no está autenticado
      setMensaje('Inicia sesión para ver la lista de pacientes.');
    }
  }, [isAuthenticated, cargarPacientes]);

  const handleEliminar = async (id: number, nombre: string) => {
    if (!isAuthenticated) {
      setMensaje('Debe iniciar sesión para realizar esta acción.');
      return;
    }
    if (window.confirm(`¿Estás seguro de que deseas eliminar a ${nombre}?`)) {
      setMensaje('Eliminando paciente...');
      try {
        const token = await getAccessTokenSilently({
          authorizationParams: { audience: process.env.REACT_APP_AUTH0_API_AUDIENCE! },
        });
        await eliminarPacienteAPI(id, token);
        setMensaje(`Paciente "${nombre}" eliminado exitosamente.`);
        cargarPacientes(); // Recargar la lista
      } catch (error: any) {
        console.error(error);
        setMensaje(error.message || 'Error desconocido al eliminar paciente');
      }
    }
  };

  const abrirModalEdicion = (paciente: Paciente) => {
    setPacienteAEditar(paciente);
    setFormDataEdicion({
      nombre: paciente.nombre,
      rut: paciente.rut,
      fecha_nacimiento: paciente.fecha_nacimiento, // Asumiendo que ya está en formato YYYY-MM-DD
    });
    setMostrarModalEdicion(true);
    setMensaje(''); 
  };

  const cerrarModalEdicion = () => {
    setMostrarModalEdicion(false);
    setPacienteAEditar(null);
  };

  const handleChangeEdicion = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormDataEdicion({ ...formDataEdicion, [e.target.name]: e.target.value });
  };

  const handleSubmitEdicion = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!pacienteAEditar || !isAuthenticated) return;

    setIsSubmittingEdicion(true);
    setMensaje('Actualizando paciente...');
    try {
      const token = await getAccessTokenSilently({
        authorizationParams: { audience: process.env.REACT_APP_AUTH0_API_AUDIENCE! },
      });
      await actualizarPacienteAPI(pacienteAEditar.id, formDataEdicion, token);
      setMensaje(`Paciente "${formDataEdicion.nombre}" actualizado exitosamente.`);
      cerrarModalEdicion();
      cargarPacientes(); 
    } catch (error: any) {
      console.error(error);
      setMensaje(error.message || 'Error desconocido al actualizar paciente');
    } finally {
      setIsSubmittingEdicion(false);
    }
  };

  if (isLoadingAuth) { // Muestra carga mientras Auth0 se inicializa
    return <p className="listar-pacientes-mensaje">Verificando autenticación...</p>;
  }

  return (
    <div className="listar-pacientes-container">
      <h2 className="listar-pacientes-title">Lista de Pacientes</h2>
      
      {!isAuthenticated && <p className="listar-pacientes-mensaje">Debes iniciar sesión para ver y gestionar pacientes.</p>}
      
      {/* Mensaje principal para la lista (carga, error general) */}
      {isAuthenticated && mensaje && !mostrarModalEdicion && (
        <p className={`listar-pacientes-mensaje ${mensaje.includes('Error') ? 'error' : (mensaje.includes('exitosamente') ? 'success' : '')}`}>
          {mensaje}
        </p>
      )}

      {isAuthenticated && cargandoLista && !pacientes.length && ( // Muestra "Cargando..." solo si está autenticado y no hay pacientes aún
         <p className="listar-pacientes-mensaje">Cargando lista de pacientes...</p>
      )}

      {isAuthenticated && !cargandoLista && pacientes.length === 0 && !mensaje.includes('Error') && (
        <p className="listar-pacientes-mensaje">No hay pacientes registrados.</p>
      )}

      {isAuthenticated && pacientes.length > 0 && (
        <table className="pacientes-table">
          <thead>
            <tr>
              <th>ID</th><th>Nombre</th><th>RUT</th><th>Fecha de Nacimiento</th><th>Acciones</th>
            </tr>
          </thead>
          <tbody>
            {pacientes.map((paciente) => (
              <tr key={paciente.id}>
                <td>{paciente.id}</td>
                <td>{paciente.nombre}</td>
                <td>{paciente.rut}</td>
                <td>{paciente.fecha_nacimiento}</td>
                <td>
                  <button className="editar-button" onClick={() => abrirModalEdicion(paciente)}>Editar</button>
                  <button className="eliminar-button" onClick={() => handleEliminar(paciente.id, paciente.nombre)}>Eliminar</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {mostrarModalEdicion && pacienteAEditar && (
        <div className="modal-edicion"> {/* Deberás crear estilos para 'modal-edicion' y 'modal-contenido' en ListarPacientes.css */}
          <div className="modal-contenido">
            <span className="cerrar-modal" onClick={cerrarModalEdicion}>&times;</span>
            {/* El título se pasa ahora al FormularioPaciente */}
            <FormularioPaciente
              formData={formDataEdicion}
              handleChange={handleChangeEdicion}
              handleSubmit={handleSubmitEdicion}
              // El mensaje del modal de edición se puede manejar aquí o pasar uno específico
              mensaje={mensaje.includes('actualizar paciente') ? mensaje : ''} 
              isSubmitting={isSubmittingEdicion}
              isAuthenticated={isAuthenticated} // El formulario de edición también necesita saber esto
              textoBoton="Actualizar Paciente"
              tituloFormulario={`Editando Paciente: ${pacienteAEditar.nombre}`}
            />
          </div>
        </div>
      )}
    </div>
  );
};

export default ListarPacientes;