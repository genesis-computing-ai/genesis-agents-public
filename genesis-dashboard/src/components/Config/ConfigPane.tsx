import React, { useState, useEffect, useRef, ReactNode } from "react";
import serialize from "form-serialize";

export interface MenuItemType {
    url: string;
    title: string;
    name?: string;
}

export interface ConfigPaneProps {
    children: ReactNode;
    config: Record<string, any>;
    currentPage: string;
    className?: string;
    onChange?: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => void;
    onPaneLeave?: () => void;
    closeButtonClass?: string;
    saveButtonClass?: string;
}

interface ChildProps {
    config: Record<string, any>;
    currentPage: string;
    onPaneLeave?: () => void;
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => void;
}

interface ConfigPaneChildElement extends React.ReactElement {
    props: ChildProps;
}

const ConfigPane: React.FC<ConfigPaneProps> = ({
    children,
    config,
    currentPage,
    className = "",
    onChange,
    onPaneLeave,
    closeButtonClass = "",
    saveButtonClass = "",
}) => {
    const formRef = useRef<HTMLFormElement>(null);

    const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
        if (onChange) {
            onChange(e);
        }
    };

    const handleLeave = () => {
        if (onPaneLeave) {
            onPaneLeave();
        }
    };

    return (
        <form ref={formRef} className={`config-pane ${className}`}>
            {React.Children.map(children, (child) => {
                if (React.isValidElement(child)) {
                    return React.cloneElement(child as ConfigPaneChildElement, {
                        config,
                        currentPage,
                        onChange: handleChange,
                        onPaneLeave: handleLeave,
                    });
                }
                return child;
            })}
        </form>
    );
};

export default ConfigPane;
