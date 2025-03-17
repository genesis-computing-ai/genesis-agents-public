<<<<<<< Updated upstream
import React from 'react';
import './App.css';
// import Projects from './components/Projects';
import Settings from './components/Settings/Settings';

function App() {
  return (
    <div className="App">
      <header className="App-header">
            <div className="App">
              <Settings />
            </div>
=======
import React, { useState, useEffect } from 'react';
import './App.css';
// import Projects from './components/Projects';
import Config from './components/Config/Config';
import SideMenu from "./components/SideMenu/SideMenu";

interface MenuItemType {
  url: string;
  title: string;
  name?: string;
}

function App() {
  const [currentPage, setCurrentPage] = useState<string>("");
  const [activeTab, setActiveTab] = useState<string>("config");
  const [lastConfigPage, setLastConfigPage] = useState<string>("/configs/llm_config");

  const handleConfigPageChange = (menuItem: MenuItemType) => {
    setCurrentPage(menuItem.url);
    if (menuItem.url !== "") {  // Only store non-empty URLs
      setLastConfigPage(menuItem.url);
    }
  };

  const handleTabChange = (tabId: string) => {
    setActiveTab(tabId);
    // When switching to config tab, restore last selected page
    if (tabId === "config") {
      setCurrentPage(lastConfigPage);
    } else {
      setCurrentPage("");
    }
  };

  // Set initial config page
  useEffect(() => {
    if (activeTab === "config" && currentPage === "") {
      setCurrentPage(lastConfigPage);
    }
  }, [activeTab, currentPage, lastConfigPage]);

  return (
    <div className="App">
      <header className="App-header">
        <div className='page-wrapper'>
          <div id='left-side-wrapper'>
            <SideMenu 
              onConfigPageChange={handleConfigPageChange} 
              currentConfigPage={currentPage}
              onTabChange={handleTabChange}
              activeTab={activeTab}
            />
          </div>
          <div id='right-side-wrapper'>
            <Config currentPage={currentPage} activeTab={activeTab} />
          </div>
        </div>
>>>>>>> Stashed changes
      </header>
    </div>
  );
}

export default App;
