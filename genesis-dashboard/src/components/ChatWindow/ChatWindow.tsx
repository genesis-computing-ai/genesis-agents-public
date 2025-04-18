import React, { useState } from 'react';
import './ChatWindow.css';

interface ChatWindowProps {
  onSubmit?: (message: string) => void;
}

const ChatWindow: React.FC<ChatWindowProps> = ({ onSubmit }) => {
  const [userInput, setUserInput] = useState('');

  const handleSubmit = () => {
    if (userInput.trim()) {
      onSubmit?.(userInput);
      setUserInput('');
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  return (
    <div className="chat-window">
      <div className="chat-messages">
        {/* Chat messages will be rendered here */}
      </div>
      <div className="chat-input-container">
        <textarea
          className="chat-input"
          value={userInput}
          onChange={(e) => setUserInput(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Type your message here..."
          rows={3}
        />
        <div
          className="submit-button"
          onClick={handleSubmit}
          role="button"
          aria-label="Submit message"
        >
          <img
            src="/send-icon.svg"
            alt="Send"
            width="24"
            height="24"
          />
        </div>
      </div>
    </div>
  );
};

export default ChatWindow;
