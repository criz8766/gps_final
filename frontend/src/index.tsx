// frontend/src/index.tsx (Fragmento importante)
import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css'; //
import App from './App';
import reportWebVitals from './reportWebVitals'; //
import { Auth0Provider } from '@auth0/auth0-react';

const auth0Domain = process.env.REACT_APP_AUTH0_DOMAIN;
const auth0ClientId = process.env.REACT_APP_AUTH0_CLIENT_ID;
const auth0ApiAudience = process.env.REACT_APP_AUTH0_API_AUDIENCE; // El audience de tu API Backend

if (!auth0Domain || !auth0ClientId || !auth0ApiAudience) {
  // Considera mostrar un error más amigable en la UI si es una app real
  console.error("Error: Variables de entorno de Auth0 no están configuradas correctamente.");
  // podrías renderizar un mensaje de error en lugar de la app
}

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    {/* Solo renderiza Auth0Provider si las variables están definidas */}
    {auth0Domain && auth0ClientId && auth0ApiAudience ? (
      <Auth0Provider
        domain={auth0Domain}
        clientId={auth0ClientId}
        authorizationParams={{
          redirect_uri: window.location.origin, // Asegúrate que esta URL está en "Allowed Callback URLs" en Auth0
          audience: auth0ApiAudience,          // CRUCIAL para obtener un token para tu API
        }}
      >
        <App />
      </Auth0Provider>
    ) : (
      <div>
        <h1>Error de Configuración</h1>
        <p>La aplicación no pudo cargarse debido a una configuración incorrecta de Auth0. Revisa las variables de entorno.</p>
      </div>
    )}
  </React.StrictMode>
);

reportWebVitals();