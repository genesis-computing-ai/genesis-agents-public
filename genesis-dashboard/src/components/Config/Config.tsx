import React, { useState, useRef } from "react";
import ConfigPane from "./ConfigPane";
import ConfigPage from "./ConfigPage";
import ConfigContent from "./ConfigContent";

interface ConfigState {
  [key: string]: string;
}

interface ConfigProps {
  currentPage: string;
  activeTab: string;
}

const Config: React.FC<ConfigProps> = ({ currentPage, activeTab }) => {
  // State for configs
  const [config, setConfig] = useState<ConfigState>({
    "myconfig.llm.llm-model": "OpenAI",
  });

  // Refs for modal and overlay
  const prefsRef = useRef<HTMLDivElement>(null);
  const overlayRef = useRef<HTMLDivElement>(null);

  type FormElement =
    | HTMLInputElement
    | HTMLSelectElement
    | HTMLTextAreaElement
    | HTMLFormElement;

  // Handle configs changes
  const handleConfigChange = (
    event: React.ChangeEvent<FormElement>
  ) => {
    const { name, value } = event.target;
    setConfig((prevConfig) => ({
      ...prevConfig,
      [name]: value,
    }));
  };

  // Handle saving configs
  const handleLeavePane = (
    wasSaved: boolean,
    newConfig: ConfigState,
    oldConfig: ConfigState
  ) => {
    if (
      wasSaved &&
      JSON.stringify(newConfig) !== JSON.stringify(oldConfig)
    ) {
      setConfig(newConfig);
    }
    hidePrefs();
  };

  // Hide configs modal
  const hidePrefs = () => {
    if (prefsRef.current && overlayRef.current) {
      prefsRef.current.className = "md-modal";
      overlayRef.current.style.visibility = "";
    }
  };

  // Show configs modal
  const showPrefs = () => {
    if (prefsRef.current && overlayRef.current) {
      prefsRef.current.className = "md-modal show";
      overlayRef.current.style.visibility = "visible";
    }
  };

  return (
    <div className="config-wrapper">
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
                defaultValue={config["myconfig.llm.llm-model"]}
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
                defaultValue=""
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
                defaultValue=""
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
                defaultValue="MyBot"
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
                defaultValue=""
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
                defaultValue=""
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/web_access_api_config">
            <fieldset className="form-group">
              <label htmlFor="webAccessKey">Web Access Key: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.web_access.dummy"
                placeholder="Web Access Key"
                id="webAccessKey"
                onChange={handleConfigChange}
                defaultValue=""
              />
            </fieldset>
          </ConfigPage>
          <ConfigPage handler="/configs/google_api_config">
            <fieldset className="form-group">
              <label htmlFor="googleApiKey">Google API Key: </label>
              <input
                type="text"
                className="form-control"
                name="myconfig.google_api.dummy"
                placeholder="Google API Key"
                id="googleApiKey"
                onChange={handleConfigChange}
                defaultValue=""
              />
            </fieldset>
          </ConfigPage>
        </ConfigContent>
      </ConfigPane>
    </div>
  );
};

export default Config;
