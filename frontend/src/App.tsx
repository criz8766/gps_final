// frontend/src/App.tsx
import React from 'react';
import './App.css'; // Este será el App.css de la "rama Javi" con la barra azul
import { useAuth0 } from '@auth0/auth0-react';
import CrearPaciente from './components/CrearPaciente'; 
import ListarPacientes from './components/ListarPacientes'; // Importamos el nuevo componente

function App() {
  const { 
    loginWithRedirect, 
    logout, 
    user, 
    isAuthenticated, 
    isLoading,
  } = useAuth0();

  if (isLoading) {
    return <div className="app-loading-container">Cargando aplicación...</div>; // Puedes estilizar esto
  }

  return (
    <div className="App"> {/* Clase raíz de App.css de la "rama Javi" */}
      <header className="App-header"> {/* Estilo de la barra azul */}
        <h1>Gestión de Pacientes</h1> {/* Título de la "rama Javi" */}
        {/* Lógica de botones de Auth0 */}
        {!isAuthenticated && (
          <button className="auth-button login" onClick={() => loginWithRedirect()}>
            Iniciar Sesión
          </button>
        )}
        {isAuthenticated && user && (
          <div className="user-profile-controls">
            <span className="user-greeting">Hola, {user.name || user.email}</span>
            <button 
              className="auth-button logout" 
              onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}
            >
              Cerrar Sesión
            </button>
          </div>
        )}
      </header>

      <main className="main-content-area"> {/* Clase para el área principal si la tienes en tu App.css */}
        {isAuthenticated ? (
          <>
            <div className="component-section"> {/* Contenedor para CrearPaciente */}
              <CrearPaciente />
            </div>
            <hr className="separador" /> {/* Separador visual de App.css de la "rama Javi" */}
            <div className="component-section"> {/* Contenedor para ListarPacientes */}
              <ListarPacientes />
            </div>
          </>
        ) : (
          <div className="login-required-prompt">
            <h2>Bienvenido al Sistema de Gestión de Pacientes</h2>
            <p>Por favor, inicia sesión para acceder a todas las funcionalidades.</p>
            {/* Podrías repetir el botón de login aquí si !isAuthenticated es true y no está en el header */}
          </div>
        )}
      </main>
      {/* Puedes añadir un footer si el diseño de la "rama Javi" lo tenía */}
      {/* <footer className="App-footer">
           <p>&copy; {new Date().getFullYear()} Farmacia Comunal</p>
         </footer> 
      */}
    </div>
  );
}

export default App;