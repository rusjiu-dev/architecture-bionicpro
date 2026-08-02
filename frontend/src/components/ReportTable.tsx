// frontend/src/components/ReportTable.tsx

import React from 'react';

interface ReportTableProps {
  records: any[];
}

const ReportTable: React.FC<ReportTableProps> = ({ records }) => {
  if (!records || records.length === 0) {
    return <p>Нет данных</p>;
  }

  const columnHeaders: { [key: string]: string } = {
    user_id: 'User ID',
    prosthesis_id: 'Протез ID',
    user_full_name: 'ФИО',
    user_email: 'Email',
    prosthesis_model: 'Модель протеза',
    gesture_count: 'Кол-во жестов',
    avg_response_time_ms: 'Среднее время (мс)',
    p95_response_time_ms: 'P95 время (мс)',
    error_count: 'Ошибки',
    error_rate: 'Доля ошибок',
    battery_health_score: 'Здоровье батареи',
    active_minutes: 'Активное время (мин)',
    report_generated_at: 'Дата генерации'
  };

  const columns = Object.keys(records[0]);

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ 
        width: '100%', 
        borderCollapse: 'collapse', 
        fontSize: '14px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
        borderRadius: '8px',
        overflow: 'hidden'
      }}>
        <thead>
          <tr style={{ background: '#2c3e50', color: 'white' }}>
            {columns.map(col => (
              <th 
                key={col} 
                style={{ 
                  padding: '12px 15px', 
                  textAlign: 'left', 
                  border: '1px solid #34495e',
                  whiteSpace: 'nowrap'
                }}
              >
                {columnHeaders[col] || col.replace(/_/g, ' ').toUpperCase()}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {records.map((record, index) => (
            <tr 
              key={index} 
              style={{ 
                background: index % 2 === 0 ? '#f8f9fa' : 'white',
                transition: 'background 0.2s'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = '#e3f2fd';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = index % 2 === 0 ? '#f8f9fa' : 'white';
              }}
            >
              {columns.map(col => {
                const value = record[col];
                let displayValue = value;
                
                // Форматирование даты
                if (col === 'report_generated_at' && value) {
                  displayValue = new Date(value).toLocaleString('ru-RU');
                }
                
                // Форматирование чисел
                if (typeof value === 'number' && col !== 'user_id' && col !== 'prosthesis_id') {
                  displayValue = Number(value).toFixed(2);
                }
                
                // Обработка null/undefined
                if (displayValue === null || displayValue === undefined) {
                  displayValue = '-';
                }
                
                // Подсветка ошибок
                if (col === 'error_count' && value > 0) {
                  return (
                    <td 
                      key={col} 
                      style={{ 
                        padding: '10px 15px', 
                        border: '1px solid #ddd',
                        color: '#c62828',
                        fontWeight: 'bold'
                      }}
                    >
                      ⚠️ {String(displayValue)}
                    </td>
                  );
                }
                
                return (
                  <td key={col} style={{ padding: '10px 15px', border: '1px solid #ddd' }}>
                    {String(displayValue)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default ReportTable;