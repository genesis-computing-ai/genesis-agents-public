import React, { ReactNode, ReactElement } from "react";

export interface ConfigPageProps {
    children: ReactNode;
    handler: string;
    config?: Record<string, any>;
    currentPage?: string;
    onChange?: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => void;
    onPaneLeave?: () => void;
    activeTab?: string;
}

interface InputProps {
    onChange?: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => void;
    value?: string;
}

const ConfigPage: React.FC<ConfigPageProps> = ({
    children,
    handler,
    config,
    currentPage,
    onChange,
    onPaneLeave,
    activeTab,
}) => {
    if (currentPage !== handler) {
        return null;
    }

    const childrenWithProps = React.Children.map(children, (child) => {
        if (React.isValidElement<InputProps>(child)) {
            return React.cloneElement(child, {
                onChange,
                value: config?.[handler],
            });
        }
        return child;
    });

    return (
        <div className="config-page">
            {childrenWithProps}
        </div>
    );
};

export default ConfigPage;
