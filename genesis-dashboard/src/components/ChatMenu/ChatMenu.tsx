import React, { useState, useRef } from 'react';
import './ChatMenu.css';

const ChatMenu: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      handleFiles(files);
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      handleFiles(files);
    }
  };

  const handleFiles = (files: FileList) => {
    // TODO: Handle the uploaded files
    console.log('Files to process:', files);
  };

  return (
    <div className="chat-menu">
      <div className="new-chat-button">⚡ New Chat</div>
      <div className="active-sessions-label">Active Chat Sessions:</div>
      <div className="active-chat-session-wrapper">
        <div className="active-chat-session">⚡ -active-bot-</div>
        <div className="active-chat-session-delete">X</div>
      </div>
      <div className="divider" />
      <div
        className={`upload-box ${isOpen ? "open" : ""}`}
        onClick={() => setIsOpen(!isOpen)}
      >
        <div className="upload-box-header">
          <span className="upload-box-header-text">Upload File</span>
          <span className="disclosure-arrow">{isOpen ? "▼" : "▶"}</span>
        </div>
        <div className="upload-box-content" onClick={(e) => e.stopPropagation()}>
          <div className="upload-box-content-text">
            FILE UPLOADER:
          </div>
          <div 
            className={`file-upload-area ${isDragging ? 'dragging' : ''}`}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
          >
            <input
              type="file"
              ref={fileInputRef}
              onChange={handleFileSelect}
              className="file-input"
              multiple
            />
            <div className="upload-instructions">
              <p>Drag & drop files here</p>
              <p>or</p>
              <button 
                className="select-files-button"
                onClick={() => fileInputRef.current?.click()}
              >
                Select Files
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="divider" />
    </div>
  );
};

export default ChatMenu;
