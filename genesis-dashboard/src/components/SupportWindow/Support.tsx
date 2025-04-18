import React from 'react';
import './Support.css';

interface SupportProps {
  onTabChange: (tabId: string) => void;
}

const Support: React.FC<SupportProps> = ({ onTabChange }) => {
  return (
    <div className="support-window">
      <div className="back-to-chat" onClick={() => onTabChange("chat")}>
        ← Back to Chat
      </div>
      <div className="support-info-header">Support & Community</div>
      <div className="divider" />
      <div className="support-info">
        <a
          href="https://genesiscomputing.ai/docs/"
          target="_blank"
          rel="noopener noreferrer"
        >
          📚 Genesis Documentation
        </a>
        <a
          href="https://genesiscomputing.ai/"
          target="_blank"
          rel="noopener noreferrer"
        >
          💬 Join our Slack Community
        </a>
      </div>
    </div>
  );
};

export default Support;
