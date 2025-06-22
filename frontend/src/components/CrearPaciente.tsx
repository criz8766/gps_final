// frontend/src/components/CrearPaciente.tsx
import React, { useState } from 'react';
import { useAuth0 } from '@auth0/auth0-react';
import { crearPacienteAPI, PacienteCreate } from '../api/pacientes';
import FormularioPaciente from './FormularioPaciente';
import './CrearPaciente.css'; // Usa esto si tienes estilos adicionales para la *sección* de crear

const CrearPaciente: React.FC = () => {
  const { getAccessTokenSilently, isAuthenticated } = useAuth0(); // isLoadingAuth no es necesario aquí si App.tsx lo maneja
  
  const [formData, setFormData] = useState<PacienteCreate>({
    nombre: '',
    rut: '',
    fecha_nacimiento: '',
  });
  const [mensaje, setMensaje] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isAuthenticated) {
      setMensaje('Debe iniciar sesión para realizar esta acción.');
      return;
    }
    setIsSubmitting(true);
    setMensaje('Registrando paciente...');
    try {
      const token = await getAccessTokenSilently({
        authorizationParams: { audience: process.env.REACT_APP_AUTH0_API_AUDIENCE! },
      });
      const pacienteCreado = await crearPacienteAPI(formData, token);
      setMensaje(`Paciente "${pacienteCreado.nombre}" creado con ID: ${pacienteCreado.id}`);
      setFormData({ nombre: '', rut: '', fecha_nacimiento: '' }); // Limpiar
    } catch (error: any) {
      setMensaje(error.message || 'Error desconocido al crear paciente.');
      console.error(error);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    // Esta clase contenedora es del CrearPaciente.css
    <div className="crear-paciente-container"> 
      {/* El título ya está en FormularioPaciente */}
      {}
      <FormularioPaciente
        formData={formData}
        handleChange={handleChange}
        handleSubmit={handleSubmit}
        mensaje={mensaje}
        isSubmitting={isSubmitting}
        isAuthenticated={isAuthenticated}
        textoBoton="Crear Paciente"
        tituloFormulario="Registrar Nuevo Paciente" 
      />
    </div>
  );
};

export default CrearPaciente;