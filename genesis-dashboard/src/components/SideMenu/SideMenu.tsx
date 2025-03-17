import React from "react";
import "./SideMenu.css";
import ConfigMenu from "../Config/ConfigMenu";

interface TabProps {
    id: string;
    label: string;
    content: React.ReactNode;
}

interface MenuItemType {
    url: string;
    title: string;
    name?: string;
}

interface SideMenuProps {
    onConfigPageChange: (menuItem: MenuItemType) => void;
    currentConfigPage: string;
    onTabChange: (tabId: string) => void;
    activeTab: string;
}

const SideMenu: React.FC<SideMenuProps> = ({ onConfigPageChange, currentConfigPage, onTabChange, activeTab }) => {
    const handleTabClick = (tabId: string) => {
        onTabChange(tabId);
    };

    const menuItems = [
        { url: "/configs/llm_config", title: "LLM Config" },
        { url: "/configs/slack_config", title: "Setup Slack Connection" },
        { url: "/configs/bot_config", title: "Bot Configuration" },
        { url: "/configs/harvester_status", title: "Harvester Status" },
        { url: "/configs/jira_config", title: "Setup Jira API Params" },
        { url: "/configs/web_access_api_config", title: "Setup WebAccess API Params" },
        { url: "/configs/google_api_config", title: "Setup Google Workspace API" },
    ];

    const handleMenuItemClick = (menuItem: MenuItemType) => {
        if (activeTab === "config") {
            onConfigPageChange(menuItem);
        }
    };

    const switchContent = (menuItem: MenuItemType) => {
        if (activeTab === "config") {
            onConfigPageChange(menuItem);
        }
    };

    // Clear config selection when switching away from config tab
    React.useEffect(() => {
        if (activeTab !== "config") {
            onConfigPageChange({ url: "", title: "" });
        }
    }, [activeTab, onConfigPageChange]);

    const TabContent: React.FC<{ tabId: string }> = ({ tabId }) => {
        switch (tabId) {
            case "chat":
                return (
                    <div className="tab-section">
                        <h3>Chat</h3>
                        <div>Chat Content</div>
                    </div>
                );
            case "projects":
                return (
                    <div className="tab-section">
                        <h3>Project Manager</h3>
                        <div>Projects Content</div>
                    </div>
                );
            case "config":
                return (
                    <div className="tab-section">
                        <ConfigMenu
                            headline="Configuration"
                            items={menuItems}
                            currentPage={currentConfigPage}
                            switchContent={switchContent}
                            onMenuItemClick={handleMenuItemClick}
                        />
                    </div>
                );
            case "help":
                return (
                    <div className="tab-section">
                        <h3>Help</h3>
                        <div>Help Content</div>
                    </div>
                );
            default:
                return null;
        }
    };

    const tabs: TabProps[] = [
        {
            id: "chat",
            label: "Chat",
            content: <TabContent tabId="chat" />,
        },
        {
            id: "projects",
            label: "Projects",
            content: <TabContent tabId="projects" />,
        },
        {
            id: "config",
            label: "Config",
            content: <TabContent tabId="config" />,
        },
        {
            id: "help",
            label: "Help",
            content: <TabContent tabId="help" />,
        },
    ];

    return (
        <div className="side-menu">
            <div className="logo-container">
                <img src="/genesis_1980_logo.png" alt="Logo" className="logo" />
            </div>
            <div className="tab-buttons">
                {tabs.map((tab) => (
                    <button
                        key={tab.id}
                        className={`tab-button ${activeTab === tab.id ? "active" : ""}`}
                        onClick={() => handleTabClick(tab.id)}
                    >
                        {tab.label}
                    </button>
                ))}
            </div>
            <div className="tab-content">
                {tabs.find((tab) => tab.id === activeTab)?.content}
            </div>
        </div>
    );
};

export default SideMenu;
