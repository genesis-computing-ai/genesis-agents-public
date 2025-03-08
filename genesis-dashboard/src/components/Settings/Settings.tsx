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
    "mysettings.general.name": "Joe Schmoe",
    "mysettings.general.username": "jschmoe",
    "mysettings.general.color-theme": "purple",
    "mysettings.general.email": "jschmoe@genesiscomputing.ai",
    "mysettings.general.picture": "earth",
    "mysettings.profile.firstname": "Joe",
    "mysettings.profile.lastname": "Schmoe",
    "mysettings.profile.biography": "",
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
    <div>
      <div className="page-header">
        <h1>Genesis Settings</h1>
      </div>
      <div style={{ margin: "30px 0 90px 0" }}>
        <button onClick={showPrefs} className="btn btn-default">
          Show Preferences
        </button>
      </div>
      <p>
        <h4>Result</h4>
        <pre className="well">{JSON.stringify(settings, null, 4)}</pre>
      </p>
      <div ref={overlayRef} className="overlay" />

      <div ref={prefsRef} className="md-modal">
        <SettingsPane
          items={menu}
          index="/settings/llm_config"
          settings={settings}
          onChange={handleSettingsChange}
          onPaneLeave={handleLeavePane}
        >
          <SettingsMenu
            headline="LLM Model & Key"
            items={menu}
            currentPage={currentPage}
            switchContent={switchContent}
            onMenuItemClick={handleMenuItemClick}
          />
          <SettingsContent header>
            <SettingsPage handler="/settings/llm_config">
              <fieldset className="form-group">
                <label htmlFor="generalName">Name: </label>
                <input
                  type="text"
                  className="form-control"
                  name="mysettings.general.name"
                  placeholder="Name"
                  id="generalName"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.general.name"]}
                />
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="generalUsername">Username: </label>
                <div className="input-group">
                  <span className="input-group-addon">@</span>
                  <input
                    type="text"
                    name="mysettings.general.username"
                    className="form-control"
                    placeholder="Username"
                    aria-describedby="basic-addon1"
                    onChange={handleSettingsChange}
                    defaultValue={settings["mysettings.general.username"]}
                  />
                </div>
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="generalMail">E-Mail address: </label>
                <input
                  type="text"
                  className="form-control"
                  name="mysettings.general.email"
                  placeholder="E-Mail Address"
                  id="generalMail"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.general.email"]}
                />
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="generalPic">Picture: </label>
                <input
                  type="text"
                  className="form-control"
                  name="mysettings.general.picture"
                  placeholder="Picture"
                  id="generalPic"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.general.picture"]}
                />
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="profileColor">Color-Theme: </label>
                <select
                  name="mysettings.general.color-theme"
                  id="profileColor"
                  className="form-control"
                  defaultValue={settings["mysettings.general.color-theme"]}
                >
                  <option value="blue">Blue</option>
                  <option value="red">Red</option>
                  <option value="purple">Purple</option>
                  <option value="orange">Orange</option>
                </select>
              </fieldset>
            </SettingsPage>
            <SettingsPage handler="/settings/slack_config">
              <fieldset className="form-group">
                <label htmlFor="profileFirstname">Firstname: </label>
                <input
                  type="text"
                  className="form-control"
                  name="mysettings.profile.firstname"
                  placeholder="Firstname"
                  id="profileFirstname"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.profile.firstname"]}
                />
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="profileLastname">Lastname: </label>
                <input
                  type="text"
                  className="form-control"
                  name="mysettings.profile.lastname"
                  placeholder="Lastname"
                  id="profileLastname"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.profile.lastname"]}
                />
              </fieldset>
              <fieldset className="form-group">
                <label htmlFor="profileBiography">Biography: </label>
                <textarea
                  className="form-control"
                  name="mysettings.profile.biography"
                  placeholder="Biography"
                  id="profileBiography"
                  onChange={handleSettingsChange}
                  defaultValue={settings["mysettings.profile.biography"]}
                />
              </fieldset>
            </SettingsPage>
          </SettingsContent>
        </SettingsPane>
      </div>
    </div>
  );
};

export default Settings;