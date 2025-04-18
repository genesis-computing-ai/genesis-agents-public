import React, { useState, useRef, useEffect } from "react";
import axios from "axios";
import ConfigPane from "./ConfigPane";
import ConfigPage from "./ConfigPage";
import ConfigContent from "./ConfigContent";

import "./ConfigWindow.css";

interface ConfigState {
  [key: string]: string;
}

interface ConfigProps {
  currentPage: string;
  activeTab: string;
  onTabChange?: (tabId: string) => void;
  setCredentials?: (service: string, params: Record<string, string>) => Promise<any>;
}

interface GoogleApiConfig {
  data: Array<[string, Array<[string, string, string, string?, string?, string?]>]>;
}

interface ApiError {
  status: number;
  message: string;
  details: any;
}

const ConfigWindow: React.FC<ConfigProps> = ({ currentPage, activeTab, onTabChange, setCredentials }) => {
  const [config, setConfig] = useState<ConfigState>({
    "myconfig.llm.llm-model": "OpenAI",
  });
  const [googleApiConfig, setGoogleApiConfig] = useState<GoogleApiConfig | null>(null);
  const [googleApiError, setGoogleApiError] = useState<ApiError | null>(null);
  const [webAccessConfig, setWebAccessConfig] = useState<GoogleApiConfig | null>(null);
  const [webAccessError, setWebAccessError] = useState<ApiError | null>(null);
  const [jiraConfig, setJiraConfig] = useState<GoogleApiConfig | null>(null);
  const [jiraError, setJiraError] = useState<ApiError | null>(null);
  const [webAccessInputs, setWebAccessInputs] = useState<Record<string, string>>({});
  const [jiraInputs, setJiraInputs] = useState<Record<string, string>>({});
  const [googleInputs, setGoogleInputs] = useState<Record<string, string>>({});

  const getCredentials = async (param: string) => {
    try {
      const payload = {
        data: [["", `get_credentials ${param}`]]
      };
      console.log('Payload: ', payload);
      const response = await axios.post(
        `${process.env.REACT_APP_GENESIS_SERVER_ENDPOINT}/udf_proxy/get_metadata`,
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          withCredentials: false
        }
      );

      // Check if the response contains an error message
      if (response.data?.data?.[0]?.[1]?.Success === false) {
        return {
          data: null,
          error: {
            status: 400,
            message: response.data.data[0][1].Message || 'Unknown error',
            details: response.data
          }
        };
      }

      return { data: response.data, error: null };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error('Axios error:', error.message, error.response?.headers);
        // Add more specific error handling for CORS issues
        if (error.code === 'ERR_NETWORK') {
          return {
            data: null,
            error: {
              status: 500,
              message: 'Unable to connect to the server. This might be due to CORS restrictions. Please check the server configuration.',
              details: {
                error: error,
                suggestion: 'The server needs to include the following CORS headers:\n' +
                          '- Access-Control-Allow-Origin: http://localhost:3001\n' +
                          '- Access-Control-Allow-Methods: POST\n' +
                          '- Access-Control-Allow-Headers: Content-Type'
              }
            }
          };
        }
        return {
          data: null,
          error: {
            status: error.response?.status || 500,
            message: `${error.message}. This might be a CORS issue - the server needs to allow requests from http://localhost:3001`,
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

  const handleInputChange = (service: string, key: string, value: string) => {
    switch (service) {
      case 'webaccess':
        setWebAccessInputs(prev => ({ ...prev, [key]: value }));
        break;
      case 'jira':
        setJiraInputs(prev => ({ ...prev, [key]: value }));
        break;
      case 'google':
        setGoogleInputs(prev => ({ ...prev, [key]: value }));
        break;
    }
  };

  useEffect(() => {
    const fetchGoogleApiConfig = async () => {
      if (currentPage === "/configs/google_api_config") {
        setGoogleApiError(null);
        const result = await getCredentials('g-sheets');
        console.log('API Response:', JSON.stringify(result, null, 2));
        if (result.error) {
          setGoogleApiError(result.error);
        } else {
          setGoogleApiConfig(result.data);
          // Initialize Google inputs with config data
          if (result.data?.data?.[0]?.[1]) {
            const initialInputs = result.data.data[0][1].reduce((acc: Record<string, string>, [_, key, value]: [string, string, string]) => {
              if (key) acc[key] = value || '';
              return acc;
            }, {});
            setGoogleInputs(initialInputs);
          }
        }
      }
    };

    const fetchWebAccessConfig = async () => {
      if (currentPage === "/configs/web_access_api_config") {
        setWebAccessError(null);
        const result = await getCredentials('serper');
        console.log('API Response:', JSON.stringify(result, null, 2));
        if (result.error) {
          setWebAccessError(result.error);
        } else {
          setWebAccessConfig(result.data);
          // Initialize WebAccess inputs with config data
          if (result.data?.data?.[0]?.[1]) {
            const initialInputs = result.data.data[0][1].reduce((acc: Record<string, string>, [_, key, value]: [string, string, string]) => {
              if (key) acc[key] = value || '';
              return acc;
            }, {});
            setWebAccessInputs(initialInputs);
          }
        }
      }
    };

    const fetchJiraConfig = async () => {
      if (currentPage === "/configs/jira_api_config") {
        setJiraError(null);
        const result = await getCredentials('jira');
        console.log('API Response:', JSON.stringify(result, null, 2));
        if (result.error) {
          setJiraError(result.error);
        } else {
          setJiraConfig(result.data);
          // Initialize Jira inputs with config data
          if (result.data?.data?.[0]?.[1]) {
            const initialInputs = result.data.data[0][1].reduce((acc: Record<string, string>, [_, key, value]: [string, string, string]) => {
              if (key) acc[key] = value || '';
              return acc;
            }, {});
            setJiraInputs(initialInputs);
          }
        }
      }
    };

    fetchGoogleApiConfig();
    fetchWebAccessConfig();
    fetchJiraConfig();
  }, [currentPage]);

  const handleConfigChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setConfig((prevConfig) => ({
      ...prevConfig,
      [name]: value,
    }));
  };

  const handleLeavePane = () => {
    // Handle leaving the pane
    console.log("Leaving pane");
  };

  return (
    <div className="config-window">
      {onTabChange && (
        <div className="back-to-chat" onClick={() => onTabChange("chat")}>
          <img
            src="/send-icon.svg"
            alt="Back"
            width="16"
            height="16"
            style={{ marginRight: "8px", transform: "rotate(180deg)" }}
          />
          ← Back to Chat
        </div>
      )}
      <ConfigPane
        currentPage={currentPage}
        config={config}
        onChange={handleConfigChange}
        onPaneLeave={handleLeavePane}
      >
        <ConfigContent
          header
          currentPage={currentPage}
          config={config}
          onChange={handleConfigChange}
          onPaneLeave={handleLeavePane}
          activeTab={activeTab}
        >
          <ConfigPage handler="/configs/llm_config">
            <h4 style={{ textAlign: "left" }}>
              Genesis Bots use OpenAI LLM models to operate. Please choose your
              OpenAI provider (OpenAI or Azure OpenAI) and API key. If you need
              an OpenAI API key, you can get one at OpenAI's website.
            </h4>
            <h4 style={{ textAlign: "left" }}>Currently Stored LLMs</h4>
            <fieldset className="form-group">
              <label htmlFor="llm-model-picker">Choose LLM Model: </label>
              <select
                name="myconfig.llm.llm-model"
                id="llm-model-picker"
                className="form-control"
                value={config["myconfig.llm.llm-model"]}
                onChange={handleConfigChange}
              >
                <option value="Open AI">Blue</option>
                <option value="Azure OpenAI">Red</option>
                <option value="Cortex">Purple</option>
              </select>
            </fieldset>
            <fieldset className="form-group">
              <label htmlFor="dummy">Name: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.llm.dummy"
                placeholder="LLM Key"
                id="dummy"
                onChange={handleConfigChange}
                value={config["myconfig.llm.dummy"] || ""}
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/slack_config">
            <fieldset className="form-group">
              <label htmlFor="slackKey">Slack Key: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.slack.dummy"
                placeholder="Slack Key"
                id="slackKey"
                onChange={handleConfigChange}
                value={config["myconfig.slack.dummy"] || ""}
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/bot_config">
            <fieldset className="form-group">
              <label htmlFor="botName">Bot Name: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.bots.dummy"
                placeholder="Bot Name"
                id="botName"
                onChange={handleConfigChange}
                value={config["myconfig.bots.dummy"] || "MyBot"}
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/harvester_config">
            <fieldset className="form-group">
              <label htmlFor="harvesterConfig">Harvester Config: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.harvester.dummy"
                placeholder="Harvester Config"
                id="harvesterConfig"
                onChange={handleConfigChange}
                value={config["myconfig.harvester.dummy"] || ""}
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/jira_config">
            <fieldset className="form-group">
              <label htmlFor="jiraKey">Jira Key: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.jira.dummy"
                placeholder="Jira Key"
                id="jiraKey"
                onChange={handleConfigChange}
                value={config["myconfig.jira.dummy"] || ""}
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/web_access_api_config">
            <h4 style={{ textAlign: "left" }}>
              Configure your WebAccess API parameters here. These will be used
              for web search and access capabilities.
            </h4>
            <div className="button-container">
              <button
                className="save-button"
                onClick={() => {
                  if (webAccessConfig?.data?.[0]?.[1] && setCredentials) {
                    const configData = webAccessConfig.data[0][1];
                    
                    // Check for empty values in the state
                    const emptyFields = Object.entries(webAccessInputs)
                      .filter(([_, value]) => !value?.trim())
                      .map(([key]) => key);
                    
                    if (emptyFields.length > 0) {
                      alert(`Please fill in all fields. Missing values for: ${emptyFields.join(', ')}`);
                      return;
                    }
                    
                    setCredentials('serper', webAccessInputs);
                  }
                }}
              >
                Save Changes
              </button>
              <button
                className="cancel-button"
                onClick={() => onTabChange && onTabChange("chat")}
              >
                Cancel
              </button>
            </div>
            {webAccessError ? (
              <div className="error-container">
                <p>
                  Error fetching WebAccess API configuration:
                  {webAccessError.message && (
                    <span>
                      <br />
                      Message: {webAccessError.message}
                    </span>
                  )}
                  {webAccessError.details && (
                    <span>
                      <br />
                      Details: {JSON.stringify(webAccessError.details)}
                    </span>
                  )}
                </p>
              </div>
            ) : webAccessConfig &&
              webAccessConfig.data &&
              webAccessConfig.data[0] &&
              webAccessConfig.data[0][1] ? (
              webAccessConfig.data[0][1].map((item, index) => {
                const [service, key, value] = item;
                const label = key || "Unknown";
                const safeName = key
                  ? key.toLowerCase().replace(/\s+/g, "_")
                  : "unknown";

                return (
                  <fieldset key={index} className="form-group">
                    <label htmlFor={`webaccess-api-${index}`}>{label}: </label>
                    <input
                      type="text"
                      className="form-control"
                      placeholder={label}
                      id={`webaccess-api-${index}`}
                      onChange={(e) => handleInputChange('webaccess', key || '', e.target.value)}
                      defaultValue={value || ""}
                    />
                  </fieldset>
                );
              })
            ) : (
              <div>Loading WebAccess API configuration...</div>
            )}
          </ConfigPage>
          <ConfigPage handler="/configs/jira_api_config">
            <h4 style={{ textAlign: "left" }}>
              Configure your Jira API parameters here. These will be used for
              Jira integration and issue tracking.
            </h4>
            <div className="button-container">
              <button
                className="save-button"
                onClick={() => {
                  if (jiraConfig?.data?.[0]?.[1] && setCredentials) {
                    const configData = jiraConfig.data[0][1];
                    
                    // Check for empty values in the state
                    const emptyFields = Object.entries(jiraInputs)
                      .filter(([_, value]) => !value?.trim())
                      .map(([key]) => key);
                    
                    if (emptyFields.length > 0) {
                      alert(`Please fill in all fields. Missing values for: ${emptyFields.join(', ')}`);
                      return;
                    }
                    
                    setCredentials('jira', jiraInputs);
                  }
                }}
              >
                Save Changes
              </button>
              <button
                className="cancel-button"
                onClick={() => onTabChange && onTabChange("chat")}
              >
                Cancel
              </button>
            </div>
            {jiraError ? (
              <div className="error-container">
                <p>
                  Error fetching Jira API configuration:
                  {jiraError.message && (
                    <span>
                      <br />
                      Message: {jiraError.message}
                    </span>
                  )}
                  {jiraError.details && (
                    <span>
                      <br />
                      Details: {JSON.stringify(jiraError.details)}
                    </span>
                  )}
                </p>
              </div>
            ) : jiraConfig &&
              jiraConfig.data &&
              jiraConfig.data[0] &&
              jiraConfig.data[0][1] ? (
              jiraConfig.data[0][1].map((item, index) => {
                const [service, key, value] = item;
                const label = key || "Unknown";
                const safeName = key
                  ? key.toLowerCase().replace(/\s+/g, "_")
                  : "unknown";

                return (
                  <fieldset key={index} className="form-group">
                    <label htmlFor={`jira-api-${index}`}>{label}: </label>
                    <input
                      type="text"
                      className="form-control"
                      placeholder={label}
                      id={`jira-api-${index}`}
                      onChange={(e) => handleInputChange('jira', key || '', e.target.value)}
                      defaultValue={value || ""}
                    />
                  </fieldset>
                );
              })
            ) : (
              <div>Loading Jira API configuration...</div>
            )}
          </ConfigPage>
          <ConfigPage handler="/configs/google_api_config">
            <h4 style={{ textAlign: "left" }}>
              Configure your Google Workspace API parameters here.
            </h4>
            <div className="button-container">
              <button
                className="save-button"
                onClick={() => {
                  if (googleApiConfig?.data?.[0]?.[1] && setCredentials) {
                    const configData = googleApiConfig.data[0][1];
                    
                    // Check for empty values in the state
                    const emptyFields = Object.entries(googleInputs)
                      .filter(([_, value]) => !value?.trim())
                      .map(([key]) => key);
                    
                    if (emptyFields.length > 0) {
                      alert(`Please fill in all fields. Missing values for: ${emptyFields.join(', ')}`);
                      return;
                    }
                    
                    setCredentials('g-sheets', googleInputs);
                  }
                }}
              >
                Save Changes
              </button>
              <button
                className="cancel-button"
                onClick={() => onTabChange && onTabChange("chat")}
              >
                Cancel
              </button>
            </div>
            {googleApiError ? (
              <div className="error-container">
                <h4 style={{ color: "#dc3545" }}>
                  Error Setting Up Google Workspace API
                </h4>
                <p style={{ color: "#dc3545" }}>
                  {googleApiError.message}
                  {googleApiError.details && (
                    <span>
                      <br />
                      Details: {JSON.stringify(googleApiError.details)}
                    </span>
                  )}
                </p>
              </div>
            ) : googleApiConfig &&
              googleApiConfig.data &&
              googleApiConfig.data[0] &&
              googleApiConfig.data[0][1] ? (
              googleApiConfig.data[0][1].map((item, index) => {
                const [service, key, value] = item;
                const label = key || "Unknown";
                const safeName = key
                  ? key.toLowerCase().replace(/\s+/g, "_")
                  : "unknown";

                return (
                  <fieldset key={index} className="form-group">
                    <label htmlFor={`google-api-${index}`}>{label}: </label>
                    <input
                      type="text"
                      className="form-control"
                      placeholder={label}
                      id={`google-api-${index}`}
                      onChange={(e) => handleInputChange('google', key || '', e.target.value)}
                      defaultValue={value || ""}
                    />
                  </fieldset>
                );
              })
            ) : (
              <div>Loading Google API configuration...</div>
            )}
          </ConfigPage>
        </ConfigContent>
      </ConfigPane>
    </div>
  );
};

export default ConfigWindow;
