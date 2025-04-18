import React from 'react';
import './ProjectsWindow.css';

interface ProjectsWindowProps {
  onTabChange: (tabId: string) => void;
}

const ProjectsWindow: React.FC<ProjectsWindowProps> = ({ onTabChange }) => {
  return (
    <div className="projects-window">
      <div className="back-to-chat" onClick={() => onTabChange("chat")}>
        ← Back to Chat
      </div>
      <div className="projects-info-header">Projects</div>
      <div className="divider" />
      <div className="projects-info">
        <p>Welcome to the Projects section. Here you can:</p>
        <ul>
          <li>View and manage your AI projects</li>
          <li>Track project status and progress</li>
          <li>Access project resources and documentation</li>
          <li>Collaborate with team members</li>
        </ul>
      </div>
    </div>
  );
};

export default ProjectsWindow;
