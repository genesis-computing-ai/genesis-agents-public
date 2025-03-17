import React, { ReactNode } from "react";

export interface ConfigPageProps {
    children: ReactNode;
    handler: string;
    config?: Record<string, any>;
    onChange?: (ev: React.ChangeEvent<HTMLFormElement>) => void;
    onPaneLeave?: (status: boolean, newConfig: any, oldConfig: any) => void;
}

const ConfigPage: React.FC<ConfigPageProps> = ({ children }) => {
    return <div className="config-page">{children}</div>;
};

export default ConfigPage;
