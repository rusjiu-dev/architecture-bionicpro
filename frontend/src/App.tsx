// frontend/src/App.tsx

import React, { useState, useEffect } from 'react';
import Keycloak from 'keycloak-js';
import ReportPage from './components/ReportPage';

const keycloak = new Keycloak({
  url: process.env.REACT_APP_KEYCLOAK_URL || 'http://localhost:8080',
  realm: process.env.REACT_APP_KEYCLOAK_REALM || 'reports-realm',
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || 'reports-frontend'
});

(window as any).keycloak = keycloak;

const App: React.FC = () => {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [user, setUser] = useState<any>(null);

  useEffect(() => {
    const initKeycloak = async () => {
      try {
        // Проверяем наличие кода в URL (фрагмент или параметры)
        const hash = window.location.hash;
        const search = window.location.search;
        const hasCode = hash.includes('code=') || search.includes('code=');
        
        console.log('[App] Has code:', hasCode);
        console.log('[App] URL:', window.location.href);

        if (hasCode) {
          // Если есть код — используем login-required для его обмена
          console.log('[App] Code found, exchanging...');
          const auth = await keycloak.init({
            onLoad: 'login-required',
            pkceMethod: 'S256',
            checkLoginIframe: false,
            redirectUri: window.location.origin + '/'
          });
          
          console.log('[App] Auth after code exchange:', auth);
          
          if (auth) {
            setAuthenticated(true);
            setUser({
              username: keycloak.tokenParsed?.preferred_username || '',
              sub: keycloak.tokenParsed?.sub || '',
              roles: keycloak.tokenParsed?.realm_access?.roles || []
            });
            // Очищаем URL от кода
            window.history.replaceState({}, document.title, window.location.pathname);
            return;
          }
        }

        // Если нет кода — пробуем check-sso
        console.log('[App] No code, checking SSO...');
        const auth = await keycloak.init({
          onLoad: 'check-sso',
          pkceMethod: 'S256',
          checkLoginIframe: false,
          redirectUri: window.location.origin + '/'
        });
        
        console.log('[App] Auth result:', auth);

        if (auth) {
          setAuthenticated(true);
          setUser({
            username: keycloak.tokenParsed?.preferred_username || '',
            sub: keycloak.tokenParsed?.sub || '',
            roles: keycloak.tokenParsed?.realm_access?.roles || []
          });
        } else {
          setAuthenticated(false);
          // Перенаправляем на логин
          console.log('[App] Redirecting to login...');
          window.location.href = keycloak.createLoginUrl({
            redirectUri: window.location.origin + '/'
          });
        }
      } catch (error) {
        console.error('[App] Keycloak error:', error);
        setAuthenticated(false);
        // При ошибке перенаправляем на логин
        window.location.href = keycloak.createLoginUrl({
          redirectUri: window.location.origin + '/'
        });
      }
    };

    initKeycloak();
  }, []);

  if (authenticated === null) {
    return (
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        height: '100vh',
        fontSize: '18px',
        color: '#666'
      }}>
        🔄 Загрузка...
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div style={{ 
        display: 'flex', 
        flexDirection: 'column',
        justifyContent: 'center', 
        alignItems: 'center', 
        height: '100vh',
        gap: '20px'
      }}>
        <h2>🔒 Требуется вход</h2>
        <button
          onClick={() => {
            window.location.href = keycloak.createLoginUrl({
              redirectUri: window.location.origin + '/'
            });
          }}
          style={{
            padding: '12px 30px',
            background: '#1976D2',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            fontSize: '16px',
            cursor: 'pointer'
          }}
        >
          Войти
        </button>
      </div>
    );
  }

  return <ReportPage user={user} />;
};

export default App;