import React, { ReactNode } from "react";

export interface ConfigContentProps {
    children: ReactNode;
    header?: boolean;
    currentPage: string;
    config: Record<string, any>;
    onChange?: (ev: React.ChangeEvent<HTMLFormElement>) => void;
    onPaneLeave?: (status: boolean, newConfig: any, oldConfig: any) => void;
    activeTab?: string;
}

interface ConfigPageProps {
    handler: string;
    config: Record<string, any>;
    onChange?: (ev: React.ChangeEvent<HTMLFormElement>) => void;
    onPaneLeave?: (status: boolean, newConfig: any, oldConfig: any) => void;
}

const ConfigContent: React.FC<ConfigContentProps> = ({
    children,
    header,
    currentPage,
    config,
    onChange,
    onPaneLeave,
    activeTab = "config",
}) => {
    const childrenArray = React.Children.toArray(children);

    const activeChild = childrenArray.find(
        (child) =>
            React.isValidElement(child) &&
            (child.props as ConfigPageProps).handler === currentPage
    );

    const getHeaderTitle = () => {
        switch (activeTab) {
            case "chat":
                return "Chat";
            case "projects":
                return "Project Manager";
            case "config":
                return "Configuration";
            case "help":
                return "Help";
            default:
                return "Configuration";
        }
    };

    return (
        <div className="config-content">
            {header && (
                <div className="config-header">
                    <div className="config-header-content">
                        <div className="config-header-title">
                            <h3>{getHeaderTitle()}</h3>
                        </div>
                    </div>
                </div>
            )}
            <div className="config-body">
                {React.isValidElement(activeChild) &&
                    React.cloneElement(activeChild as React.ReactElement<ConfigPageProps>, {
                        config,
                        onChange,
                        onPaneLeave,
                    })}
            </div>
        </div>
    );
};

export default ConfigContent;
