import React, { useEffect } from "react";

interface SettingsPageProps {
    handler: string;
    options?: Record<string, any>;
    settings?: Record<string, any>;
    onChange?: () => void;
    switchContent?: () => void;
    onPaneLeave?: () => void;
    onMenuItemClick?: () => void;
    children?: React.ReactNode;
}

const SettingsPage: React.FC<SettingsPageProps> = ({
    handler,
    options,
    settings,
    onChange,
    switchContent,
    onPaneLeave,
    onMenuItemClick,
    children,
}) => {
    const updateForm = () => {
        if (children && settings) {
            Object.keys(settings).forEach((key) => {
                const elements = document.getElementsByName(key);
                if (elements.length > 0 && elements[0]) {
                    (elements[0] as HTMLInputElement).value = settings[key];
                }
            });
        }
    };

    useEffect(() => {
        updateForm();
    }, [settings, children]);

    const renderWithOptions = () => {
        // todo: set onChange={onChange} to all form elements.
        return <div>Render With Options (not implemented, yet)</div>;
    };

    const renderWithContent = (content: React.ReactNode) => {
        return content;
    };

    const content = () => {
        if (options) {
            return renderWithOptions();
        } else if (children) {
            return renderWithContent(children);
        }
    };

    return <div className="scroller settings-innerpage">{content()}</div>;
};

export default SettingsPage;
