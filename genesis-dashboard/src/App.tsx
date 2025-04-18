import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';
// import Projects from './components/Projects';
import ConfigWindow from './components/Config/ConfigWindow';
import SideMenu from "./components/SideMenu/SideMenu";
import ChatWindow from './components/ChatWindow/ChatWindow';
import DBConnectionsWindow from './components/DBConnectionsWindow/DBConnectionsWindow';
import Support from './components/SupportWindow/Support';
import ProjectsWindow from './components/ProjectsWindow/ProjectsWindow';
import BotDocsWindow from './components/BotDocsWindow/BotDocsWindow';

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

  const handleChatSubmit = (message: string) => {
    // TODO: Implement chat message handling
    console.log('Chat message:', message);
  };

  const getCredentials = async (param: string) => {
    try {
      const payload = {
        data: [["", `get_credentials ${param}`]]
      };
      console.log('Payload: ', payload);
      const response = await axios.post(
        `${process.env.REACT_APP_GENESIS_SERVER_ENDPOINT}/udf_proxy/get_metadata`,
        payload
      );
      return { data: response.data, error: null };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error('Axios error:', error.message);
        return {
          data: null,
          error: {
            status: error.response?.status || 500,
            message: error.message,
            details: error.response?.data
          }
        };
      } else {
        console.error('Non-Axios error:', error);
        return {
          data: null,
          error: {
            status: 500,
            message: 'An unexpected error occurred',
            details: error
          }
        };
      }
    }
  };

  const setCredentials = async (service: string, params: Record<string, string>) => {
    try {
      // Create the list string starting with set_credentials and service
      let list = `set_credentials ${service}`;
      
      // Add each key-value pair to the list
      Object.entries(params).forEach(([key, value]) => {
        list += ` ${key} ${value}`;
      });

      const payload = {
        data: [["", list]]
      };
      console.log('Set Credentials Payload:', payload);

      const response = await axios.post(
        `${process.env.REACT_APP_GENESIS_SERVER_ENDPOINT}/udf_proxy/set_metadata`,
        payload
      );
      return { data: response.data, error: null };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error('Axios error:', error.message);
        return {
          data: null,
          error: {
            status: error.response?.status || 500,
            message: error.message,
            details: error.response?.data
          }
        };
      } else {
        console.error('Non-Axios error:', error);
        return {
          data: null,
          error: {
            status: 500,
            message: 'An unexpected error occurred',
            details: error
          }
        };
      }
    }
  };

  // Set initial config page
  useEffect(() => {
    if (activeTab === "config" && currentPage === "") {
      setCurrentPage(lastConfigPage);
    }
  }, [activeTab, currentPage, lastConfigPage]);

  const renderRightContent = () => {
    switch (activeTab) {
      case "chat":
        return <ChatWindow onSubmit={handleChatSubmit} />;
      case "dbs":
        return <DBConnectionsWindow onTabChange={handleTabChange} />;
      case "support":
        return <Support onTabChange={handleTabChange} />;
      case "projects":
        return <ProjectsWindow onTabChange={handleTabChange} />;
      case "botdocs":
        return <BotDocsWindow onTabChange={handleTabChange} />;
      default:
        return <ConfigWindow 
          currentPage={currentPage} 
          activeTab={activeTab} 
          onTabChange={handleTabChange}
          setCredentials={setCredentials}
        />;
    }
  };

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
            {renderRightContent()}
          </div>
        </div>
      </header>
    </div>
  );
}

export default App;
