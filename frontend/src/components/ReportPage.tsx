import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reportData, setReportData] = useState<any>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      // Проверяем, не истёк ли токен
      if (keycloak.isTokenExpired()) {
        await keycloak.updateToken(30); // обновить за 30 секунд до истечения
      }

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          // Токен недействителен — пробуем обновить
          try {
            await keycloak.updateToken(0);
            // Повторяем запрос с новым токеном
            const retryResponse = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
              headers: {
                'Authorization': `Bearer ${keycloak.token}`
              }
            });
            
            if (!retryResponse.ok) {
              throw new Error('Authentication failed after token refresh');
            }
            
            const retryData = await retryResponse.json();
            setReportData(retryData);
            return;
          } catch (refreshError) {
            keycloak.logout();
            return;
          }
        }
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      setReportData(data);

      // Если нужно скачать файл:
      if (data.fileUrl) {
        const fileResponse = await fetch(data.fileUrl, {
          headers: {
            'Authorization': `Bearer ${keycloak.token}`
          }
        });
        const blob = await fileResponse.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `report-${new Date().toISOString()}.xlsx`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const loginWithPKCE = () => {
    // Явный вызов логина с PKCE параметрами
    keycloak.login({
      redirectUri: window.location.origin,
      pkceMethod: 'S256'
    });
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md text-center">
          <h1 className="text-2xl font-bold mb-4">Usage Reports Portal</h1>
          <p className="text-gray-600 mb-6">Please login to access your reports</p>
          <button
            onClick={loginWithPKCE}
            className="px-6 py-3 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
          >
            Login with Secure PKCE
          </button>
        </div>
      </div>
    );
  }

  const userRoles = keycloak.tokenParsed?.realm_access?.roles || [];
  const canAccessReports = userRoles.includes('user') || 
                          userRoles.includes('prothetic_user') || 
                          userRoles.includes('administrator');

  if (!canAccessReports) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md">
          <h1 className="text-2xl font-bold mb-4">Access Denied</h1>
          <p className="text-gray-600">You don't have permission to access reports.</p>
          <button
            onClick={() => keycloak.logout()}
            className="mt-4 px-4 py-2 bg-red-500 text-white rounded"
          >
            Logout
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-2xl">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-2xl font-bold">Usage Reports</h1>
            <p className="text-gray-600">
              Welcome, {keycloak.tokenParsed?.preferred_username}
            </p>
            <p className="text-sm text-gray-500">
              Roles: {userRoles.join(', ')}
            </p>
          </div>
          <button
            onClick={() => keycloak.logout()}
            className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600"
          >
            Logout
          </button>
        </div>

        <div className="space-y-4">
          <button
            onClick={downloadReport}
            disabled={loading}
            className={`w-full px-4 py-3 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors ${
              loading ? 'opacity-50 cursor-not-allowed' : ''
            }`}
          >
            {loading ? (
              <span className="flex items-center justify-center">
                <svg className="animate-spin h-5 w-5 mr-3" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Generating Secure Report...
              </span>
            ) : (
              'Download Report (PKCE Secured)'
            )}
          </button>

          {reportData && (
            <div className="mt-6 p-4 bg-green-50 rounded">
              <h3 className="font-semibold text-green-800">Report Generated Successfully</h3>
              <pre className="mt-2 text-sm text-green-700">
                {JSON.stringify(reportData, null, 2)}
              </pre>
            </div>
          )}
        </div>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            <p className="font-semibold">Error</p>
            <p className="text-sm">{error}</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;