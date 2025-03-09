import React, { useState, useRef } from "react";
import {
  SettingsPane,
  SettingsPage,
  SettingsContent,
  SettingsMenu,
} from "./index";

interface SettingsState {
  [key: string]: string;
}

const Settings: React.FC = () => {
  // State for settings
  const [settings, setSettings] = useState<SettingsState>({
    "mysettings.llm.llm-model": "OpenAI",
  });

  const [currentPage, setCurrentPage] = useState<string>("/settings/general");

  // Refs for modal and overlay
  const prefsRef = useRef<HTMLDivElement>(null);
  const overlayRef = useRef<HTMLDivElement>(null);

  type FormElement =
    | HTMLInputElement
    | HTMLSelectElement
    | HTMLTextAreaElement
    | HTMLFormElement;

  const switchContent = (menuItem: { url: string }) => {
    setCurrentPage(menuItem.url);
  };

  const handleMenuItemClick = (menuItem: { url: string }) => {
    switchContent(menuItem);
  };

  // Handle settings changes
  const handleSettingsChange = (
    event: React.ChangeEvent<
      FormElement
    >
  ) => {
    const { name, value } = event.target;
    setSettings((prevSettings) => ({
      ...prevSettings,
      [name]: value,
    }));
  };

  // Handle saving settings
  const handleLeavePane = (
    wasSaved: boolean,
    newSettings: SettingsState,
    oldSettings: SettingsState
  ) => {
    if (
      wasSaved &&
      JSON.stringify(newSettings) !== JSON.stringify(oldSettings)
    ) {
      setSettings(newSettings);
    }
    hidePrefs();
  };

  // Menu items
  const menu = [
    { title: "LLM Model & Key", url: "/settings/llm_config" },
    { title: "Setup Slack Connection", url: "/settings/slack_config" },
    { title: "Bot Configuration", url: "/settings/bot_config" },
    { title: "Harvester Status", url: "/settings/harvester_config" },
    { title: "Setup Jira API Params", url: "/settings/jira_config" },
    { title: "Setup WebAccess API Params", url: "/settings/web_access_api_config" },
    { title: "Setup Google Workspace API", url: "/settings/google_api_config" },
  ];

  // Hide settings modal
  const hidePrefs = () => {
    if (prefsRef.current && overlayRef.current) {
      prefsRef.current.className = "md-modal";
      overlayRef.current.style.visibility = "";
    }
  };

  // Show settings modal
  const showPrefs = () => {
    if (prefsRef.current && overlayRef.current) {
      prefsRef.current.className = "md-modal show";
      overlayRef.current.style.visibility = "visible";
    }
  };

  return (
    <div className="settings-wrapper">
      <SettingsPane
        items={menu}
        index="/settings/llm_config"
        settings={settings}
        onChange={handleSettingsChange}
        onPaneLeave={handleLeavePane}
      >
        <SettingsMenu
          headline="Config"
          items={menu}
          currentPage={currentPage}
          switchContent={switchContent}
          onMenuItemClick={handleMenuItemClick}
        />
        <SettingsContent header>
          <SettingsPage handler="/settings/llm_config">
            <h4 style={{ textAlign: "left" }}>
              Genesis Bots use OpenAI LLM models to operate. Please choose your
              OpenAI provider (OpenAI or Azure OpenAI) and API key. If you need
              an OpenAI API key, you can get one at OpenAI's website.
            </h4>
            <h4 style={{ textAlign: "left" }}>Currently Stored LLMs</h4>
            <fieldset className="form-group">
              <label htmlFor="llm-model-picker">Choose LLM Model: </label>
              <select
                name="mysettings.llm.llm-model"
                id="llm-model-picker"
                className="form-control"
                defaultValue={settings["mysettings.llm.llm-model"]}
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
                name="mysettings.llm.dummy"
                placeholder="LLM Key"
                id="dummy"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.llm.key"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/slack_config">
            <fieldset className="form-group">
              <label htmlFor="slackKey">Slack Key: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.slack.dummy"
                placeholder="Slack Key"
                id="slackKey"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/bot_config">
            <fieldset className="form-group">
              <label htmlFor="botName">Bot Name: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.bots.dummy"
                placeholder="Bot Name"
                id="botName"
                onChange={handleSettingsChange}
                defaultValue="MyBot" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/harvester_config">
            <fieldset className="form-group">
              <label htmlFor="harvesterConfig">Harvester Config: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.harvester.dummy"
                placeholder="Harvester Config"
                id="harvesterConfig"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/jira_config">
            <fieldset className="form-group">
              <label htmlFor="jiraKey">Jira Key: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.jita.dummy"
                placeholder="Jira Key"
                id="jiraKey"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/web_access_api_config">
            <fieldset className="form-group">
              <label htmlFor="webAccessKey">Web Access Key: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.webaccess.dummy"
                placeholder="Web Access Key"
                id="webAccessKey"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
          <SettingsPage handler="/settings/google_api_config">
            <fieldset className="form-group">
              <label htmlFor="googleKey">Google Key: </label>
              <input
                type="text"
                className="form-control"
                name="mysettings.google.dummy"
                placeholder="Google Key"
                id="googleKey"
                onChange={handleSettingsChange}
                defaultValue="" // {settings["mysettings.profile.firstname"]}
              />
            </fieldset>
          </SettingsPage>
        </SettingsContent>
      </SettingsPane>
    </div>
  );
};

export default Settings;