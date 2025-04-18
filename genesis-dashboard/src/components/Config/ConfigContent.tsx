import React, { ReactNode, ReactElement } from "react";
import type { ConfigPageProps } from "./ConfigPage";

interface ConfigContentProps {
    children: ReactNode;
    config: Record<string, any>;
    currentPage: string;
    header?: boolean;
    onChange?: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => void;
    onPaneLeave?: () => void;
    activeTab?: string;
}

const ConfigContent: React.FC<ConfigContentProps> = ({
    children,
    config,
    currentPage,
    header = false,
    onChange,
    onPaneLeave,
    activeTab,
}) => {
    const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
        if (onChange) {
            onChange(e);
        }
    };

    const childrenWithProps = React.Children.map(children, (child) => {
        if (React.isValidElement<ConfigPageProps>(child)) {
            return React.cloneElement(child, {
                config,
                currentPage,
                onChange: handleChange,
                onPaneLeave,
                activeTab,
            });
        }
        return child;
    });

    return (
      <div className="config-content">
        {header && (
          <div className="config-header">
            <div className="config-header-content">
              <div className="config-header-title">Configuration</div>
            </div>
            <div className="divider" />
          </div>
        )}
        <div className="config-body">{childrenWithProps}</div>
      </div>
    );
};

export default ConfigContent;
