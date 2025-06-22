// frontend/src/components/FormularioPaciente.tsx
import React from 'react';
import { PacienteCreate } from '../api/pacientes';

interface Props {
  formData: PacienteCreate;
  handleChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  handleSubmit: (e: React.FormEvent) => void;
  mensaje: string;
  isSubmitting?: boolean;
  isAuthenticated?: boolean; // La necesitará si el botón de submit se controla aquí
  textoBoton?: string;
  tituloFormulario?: string;
}

const FormularioPaciente: React.FC<Props> = ({ 
  formData, 
  handleChange, 
  handleSubmit, 
  mensaje, 
  isSubmitting = false,
  isAuthenticated = true, // Asumimos que si se muestra el form, el usuario está auth. El padre lo controla.
  textoBoton = "Guardar",
  tituloFormulario = "Datos del Paciente"
}) => {
  return (
    <form onSubmit={handleSubmit} className="formulario-paciente-card"> {/* Clase para darle estilo de tarjeta */}
      <h3 className="formulario-paciente-titulo">{tituloFormulario}</h3> {/* Título dentro del form */}
      <div className="input-group">
        <label className="input-label">Nombre:</label>
        <input 
          type="text" name="nombre" value={formData.nombre} onChange={handleChange} 
          className="input-field" disabled={isSubmitting} required 
        />
      </div>
      <div className="input-group">
        <label className="input-label">RUT:</label>
        <input 
          type="text" name="rut" value={formData.rut} onChange={handleChange} 
          className="input-field" disabled={isSubmitting} required 
        />
      </div>
      <div className="input-group">
        <label className="input-label">Fecha de Nacimiento:</label>
        <input 
          type="date" name="fecha_nacimiento" value={formData.fecha_nacimiento} onChange={handleChange} 
          className="input-field" disabled={isSubmitting} required 
        />
      </div>
      <button 
        type="submit" 
        className="submit-button" 
        disabled={isSubmitting || !isAuthenticated} // Deshabilitar si no está autenticado o si se está enviando
      >
        {isSubmitting ? 'Enviando...' : textoBoton}
      </button>
      {mensaje && <p className="mensaje-formulario">{mensaje}</p>} {/* Clase específica para el mensaje del form */}
    </form>
  );
};

export default FormularioPaciente;