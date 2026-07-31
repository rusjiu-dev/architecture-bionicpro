import React from 'react';
import { ReactKeycloakProvider } from '@react-keycloak/web';
import Keycloak, { KeycloakConfig } from 'keycloak-js';
import ReportPage from './components/ReportPage';

// Генерация криптографически безопасного code verifier
function generateCodeVerifier(): string {
  const array = new Uint8Array(64);
  window.crypto.getRandomValues(array);
  return base64URLEncode(array);
}

// Создание code challenge из verifier (SHA-256 + Base64URL)
async function generateCodeChallenge(verifier: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(verifier);
  const hash = await window.crypto.subtle.digest('SHA-256', data);
  return base64URLEncode(new Uint8Array(hash));
}

// Кодирование в Base64URL (без '+' '/' '=')
function base64URLEncode(buffer: Uint8Array): string {
  const base64 = btoa(String.fromCharCode(...Array.from(buffer)));
  return base64
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

const keycloakConfig: KeycloakConfig = {
  url: process.env.REACT_APP_KEYCLOAK_URL,
  realm: process.env.REACT_APP_KEYCLOAK_REALM || "",
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || ""
};

const keycloak = new Keycloak(keycloakConfig);

// Настройка PKCE параметров перед инициализацией
keycloak.onAuthRefreshSuccess = () => {
  console.log('Token refreshed successfully');
};

keycloak.onAuthRefreshError = () => {
  console.error('Token refresh failed');
  keycloak.logout();
};

const App: React.FC = () => {
  const [initialized, setInitialized] = React.useState(false);

  React.useEffect(() => {
    const initKeycloak = async () => {
      try {
        // Генерируем PKCE параметры
        const codeVerifier = generateCodeVerifier();
        const codeChallenge = await generateCodeChallenge(codeVerifier);

        // Инициализируем Keycloak с PKCE
        const authenticated = await keycloak.init({
          onLoad: 'check-sso',
          silentCheckSsoRedirectUri: 
            window.location.origin + '/silent-check-sso.html',
          pkceMethod: 'S256',
          checkLoginIframe: false,
          // Передаём сгенерированные PKCE параметры
          adapter: {
            codeVerifier: codeVerifier,
            codeChallenge: codeChallenge,
          } as any
        });

        console.log(authenticated ? 'Authenticated' : 'Not authenticated');
        setInitialized(true);
      } catch (error) {
        console.error('Keycloak initialization failed:', error);
        setInitialized(true);
      }
    };

    initKeycloak();
  }, []);

  if (!initialized) {
    return <div>Initializing Keycloak...</div>;
  }

  return (
    <ReactKeycloakProvider authClient={keycloak}>
      <div className="App">
        <ReportPage />
      </div>
    </ReactKeycloakProvider>
  );
};

export default App;