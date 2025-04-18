import React from 'react';
import './BotDocsWindow.css';

interface BotDocsWindowProps {
  onTabChange: (tabId: string) => void;
}

const BotDocsWindow: React.FC<BotDocsWindowProps> = ({ onTabChange }) => {
  return (
    <div className="botdocs-window">
      <div className="back-to-chat" onClick={() => onTabChange("chat")}>
        ← Back to Chat
      </div>
      <div className="botdocs-info-header">Bot Documents Index Manager</div>
      <div className="divider" />
      <div className="botdocs-info">
        <p>Welcome to the Bot Documentation. Here you can find:</p>
        <ul>
          <li>Bot capabilities and features</li>
          <li>Integration guides</li>
          <li>API documentation</li>
          <li>Best practices and examples</li>
        </ul>
      </div>
    </div>
  );
};

export default BotDocsWindow;
