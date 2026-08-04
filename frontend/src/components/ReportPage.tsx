// frontend/src/components/ReportPage.tsx

import React, { useState, useEffect } from 'react';
import ReportTable from './ReportTable';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8081';

interface ReportPageProps {
  user?: {
    username: string;
    sub: string;
    roles: string[];
  };
}

const ReportPage: React.FC<ReportPageProps> = ({ user }) => {
  const [reportData, setReportData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [userInput, setUserInput] = useState('');

  useEffect(() => {
    const today = new Date();
    const thirtyDaysAgo = new Date(today);
    thirtyDaysAgo.setDate(today.getDate() - 30);
    
    setDateTo(today.toISOString().split('T')[0]);
    setDateFrom(thirtyDaysAgo.toISOString().split('T')[0]);
    
    if (user?.username) {
      setUserInput(user.username);
    }
  }, [user]);

  const fetchReport = async () => {
    if (!user) {
      setError('User not authenticated');
      return;
    }

    if (!userInput.trim()) {
      setError('Please enter user ID');
      return;
    }

    setLoading(true);
    setError(null);
    setReportData(null);

    try {
      const keycloak = (window as any).keycloak;
      let token = keycloak?.token;

      if (!token) {
        const response = await fetch('http://localhost:8080/realms/reports-realm/protocol/openid-connect/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            client_id: 'reports-frontend',
            grant_type: 'password',
            username: 'prothetic1',
            password: 'prothetic123'
          })
        });
        const data = await response.json();
        token = data.access_token;
      }

      const url = new URL(`${API_URL}/api/v1/reports`);
      url.searchParams.append('user_id', userInput);
      if (dateFrom) url.searchParams.append('date_from', dateFrom);
      if (dateTo) url.searchParams.append('date_to', dateTo);

      const response = await fetch(url.toString(), {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      setReportData(data);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch report');
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    console.log('[Logout] Starting logout...');
    
    // 1. Очищаем локальное хранилище
    localStorage.clear();
    sessionStorage.clear();
    
    // 2. Получаем Keycloak
    const keycloak = (window as any).keycloak;
    
    if (keycloak) {
      try {
        // 3. Очищаем токены
        keycloak.clearToken();
        
        // 4. Создаём URL для выхода
        const logoutUrl = keycloak.createLogoutUrl({
          redirectUri: window.location.origin + '/'
        });
        
        console.log('[Logout] Redirecting to:', logoutUrl);
        
        // 5. Перенаправляем на выход
        window.location.href = logoutUrl;
      } catch (err) {
        console.error('[Logout] Error:', err);
        window.location.href = window.location.origin + '/';
      }
    } else {
      window.location.href = window.location.origin + '/';
    }
  };

  if (!user) {
    return (
      <div style={{ padding: '40px', textAlign: 'center' }}>
        <h2>🔒 Please login</h2>
      </div>
    );
  }

  return (
    <div style={{ padding: '20px', maxWidth: '1400px', margin: '0 auto' }}>
      {/* Заголовок с кнопкой выхода */}
      <div style={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center',
        marginBottom: '20px',
        flexWrap: 'wrap',
        gap: '10px'
      }}>
        <h1 style={{ fontSize: '28px', color: '#1a237e', margin: 0 }}>
          📊 Протезный отчёт
        </h1>
        <button
          onClick={handleLogout}
          style={{
            padding: '8px 20px',
            background: '#c62828',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: '500',
            transition: 'background 0.2s'
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = '#b71c1c';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = '#c62828';
          }}
        >
          🚪 Выйти
        </button>
      </div>
      
      {/* Информация о пользователе */}
      <div style={{ 
        marginBottom: '20px', 
        padding: '15px', 
        background: '#f5f5f5', 
        borderRadius: '8px',
        borderLeft: '4px solid #1976D2'
      }}>
        <div><strong>👤 Пользователь:</strong> {user?.username || 'Неизвестно'}</div>
        <div><strong>🛡️ Роли:</strong> {user?.roles?.join(', ') || 'Нет ролей'}</div>
        <div style={{ fontSize: '12px', color: '#666', marginTop: '5px' }}>
          <strong>🆔 Sub:</strong> {user?.sub || 'Неизвестно'}
        </div>
      </div>

      {/* Форма запроса */}
      <div style={{ 
        display: 'flex', 
        gap: '15px', 
        flexWrap: 'wrap', 
        marginBottom: '20px', 
        alignItems: 'end',
        padding: '20px',
        background: '#fafafa',
        borderRadius: '8px',
        border: '1px solid #e0e0e0'
      }}>
        <div>
          <label style={{ display: 'block', marginBottom: '5px', fontWeight: '500' }}>User ID:</label>
          <input
            type="text"
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            placeholder="prothetic1"
            style={{ 
              padding: '8px 12px', 
              width: '200px',
              border: '1px solid #ccc',
              borderRadius: '4px',
              fontSize: '14px'
            }}
          />
        </div>
        <div>
          <label style={{ display: 'block', marginBottom: '5px', fontWeight: '500' }}>Дата от:</label>
          <input
            type="date"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
            style={{ padding: '8px 12px', border: '1px solid #ccc', borderRadius: '4px' }}
          />
        </div>
        <div>
          <label style={{ display: 'block', marginBottom: '5px', fontWeight: '500' }}>Дата до:</label>
          <input
            type="date"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
            style={{ padding: '8px 12px', border: '1px solid #ccc', borderRadius: '4px' }}
          />
        </div>
        <button
          onClick={fetchReport}
          disabled={loading}
          style={{
            padding: '10px 28px',
            background: loading ? '#90a4ae' : '#4CAF50',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: loading ? 'not-allowed' : 'pointer',
            fontSize: '16px',
            fontWeight: '500',
            transition: 'background 0.2s'
          }}
          onMouseEnter={(e) => {
            if (!loading) e.currentTarget.style.background = '#388E3C';
          }}
          onMouseLeave={(e) => {
            if (!loading) e.currentTarget.style.background = '#4CAF50';
          }}
        >
          {loading ? '⏳ Загрузка...' : '📥 Получить отчёт'}
        </button>
      </div>

      {/* Ошибка */}
      {error && (
        <div style={{ 
          padding: '15px', 
          background: '#f44336', 
          color: 'white', 
          borderRadius: '4px', 
          marginBottom: '20px'
        }}>
          ❌ {error}
        </div>
      )}

      {/* Результат */}
      {reportData && (
        <div>
          <div style={{ 
            padding: '15px', 
            background: reportData.status === 'not_found' ? '#fff3cd' : '#d4edda',
            borderRadius: '4px',
            marginBottom: '15px',
            border: `1px solid ${reportData.status === 'not_found' ? '#ffc107' : '#28a745'}`
          }}>
            <div>
              <strong>Статус:</strong> 
              {reportData.status === 'success' ? ' ✅ Отчёт готов' : ' ⏳ Отчёт не найден'}
            </div>
            {reportData.message && (
              <div style={{ marginTop: '5px', color: '#856404' }}>
                {reportData.message}
              </div>
            )}
            {reportData.total_records !== undefined && (
              <div style={{ marginTop: '5px' }}>
                📊 Записей: {reportData.total_records}
              </div>
            )}
            {reportData.period && (
              <div style={{ marginTop: '5px', fontSize: '14px', color: '#555' }}>
                📅 Период: {reportData.period.date_from} — {reportData.period.date_to}
              </div>
            )}
          </div>

          {reportData.records && reportData.records.length > 0 && (
            <ReportTable records={reportData.records} />
          )}

          {reportData.records && reportData.records.length === 0 && (
            <div style={{ 
              padding: '40px', 
              textAlign: 'center', 
              color: '#666',
              background: '#fafafa',
              borderRadius: '8px',
              border: '1px dashed #ccc'
            }}>
              <div style={{ fontSize: '48px', marginBottom: '10px' }}>📭</div>
              <p>Нет данных за выбранный период.</p>
              <small>Убедитесь, что Airflow уже обработал эти данные.</small>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default ReportPage;