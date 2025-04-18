import React from 'react';
import './DBConnectionsWindow.css';

interface DBConnectionsWindowProps {
  onTabChange: (tabId: string) => void;
}

const DBConnectionsWindow: React.FC<DBConnectionsWindowProps> = ({ onTabChange }) => {
  return (
    <div className="db-connections-window">
      <div className="back-to-chat" onClick={() => onTabChange("chat")}>
        ← Back to Chat
      </div>
      <div className="db-info-wrapper">
        <div className="management-info-header">Database Connections</div>
        <div className="table-container">
          <table className="connections-table">
            <thead>
              <tr>
                <th>Allowed Bots</th>
                <th>Connection ID</th>
                <th>Connection String</th>
                <th>Created</th>
                <th>Database Type</th>
                <th>Description</th>
                <th>Owner Bot</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Bot1, Bot2</td>
                <td>conn_001</td>
                <td>postgresql://user:pass@host:5432/db</td>
                <td>2025-03-19 10:00</td>
                <td>PostgreSQL</td>
                <td>Production Database</td>
                <td>MainBot</td>
                <td>2025-03-19 10:30</td>
              </tr>
              <tr>
                <td>Bot3</td>
                <td>conn_002</td>
                <td>mysql://user:pass@localhost:3306/dev</td>
                <td>2025-03-19 09:00</td>
                <td>MySQL</td>
                <td>Development Database</td>
                <td>TestBot</td>
                <td>2025-03-19 09:45</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className="divider" />
        <div className="management-info">
          <h1>Managing Database Connections</h1>
          <div className="management-info-instructions">
            <p>
              To add, change, or remove a database connection, please talk to
              Eve and tell her what type of database you want to connect to. She
              can help you with:
            </p>
            <ul>
              <li>Setting up new database connections</li>
              <li>Modifying existing connections</li>
              <li>Removing unused connections</li>
              <li>Configuring access permissions</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DBConnectionsWindow;
