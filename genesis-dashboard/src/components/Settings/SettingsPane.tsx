import React, { useState, useEffect, useRef, ReactNode } from "react";
import serialize from "form-serialize";

interface SettingsPaneProps {
    children: ReactNode;
    settings: Record<string, any>;
    items: Array<any>;
    index: string;
    className?: string;
    onChange?: (ev: React.ChangeEvent<HTMLFormElement>) => void;
    onPaneLeave?: (status: boolean, newSettings: any, oldSettings: any) => void;
    onMenuItemClick?: (menuItem: any) => void;
    closeButtonClass?: string;
    saveButtonClass?: string;
}

interface ChildProps {
  items: Array<any>;
  settings: Record<string, any>;
  currentPage: string;
  onPaneLeave?: (status: boolean, newSettings: any, oldSettings: any) => void;
  onMenuItemClick?: (menuItem: any) => void;
  switchContent: (menuItem: any) => void;
  onChange: (ev: React.ChangeEvent<HTMLFormElement>) => void;
}

interface SettingsPaneChildElement extends React.ReactElement {
  props: ChildProps;
}

const SettingsPane: React.FC<SettingsPaneProps> = ({
    children,
    settings,
    items,
    index,
    className,
    onChange,
    onPaneLeave,
    onMenuItemClick,
    closeButtonClass,
    saveButtonClass,
}) => {
    const [currentPage, setCurrentPage] = useState(index);
    const [currentSettings, setCurrentSettings] = useState(settings);
    const formRef = useRef<HTMLFormElement | null>(null);
    let keyUpListener: { remove: () => void } | null = null;

    const addEvent = (node: Document, event: string, handler: (ev: KeyboardEvent) => void) => {
        // Change to explicitly type the handler as EventListener
        const typedHandler: EventListener = handler as EventListener;
        node.addEventListener(event, typedHandler);
        return {
            remove() {
                node.removeEventListener(event, typedHandler);
            },
        };
    };

    const handleKeyUp = (ev: KeyboardEvent) => {
        if (ev.key === "Escape") {
            onPaneLeave?.(false, currentSettings, currentSettings);
            keyUpListener?.remove();
        }
    };

    const load = () => {
        keyUpListener = addEvent(document, "keyup", handleKeyUp);
    };

    useEffect(() => {
        load();
        return () => {
            keyUpListener?.remove();
        };
    }, []);

    useEffect(() => {
        load();
    });

    const switchContent = (menuItem: any) => {
        if (currentPage !== menuItem.url) {
            setCurrentPage(menuItem.url);
        }
    };

    const settingsChanged = (ev: React.ChangeEvent<HTMLFormElement>) => {
        onChange?.(ev);
    };

    const handleSubmit = (ev: React.FormEvent<HTMLFormElement>) => {
        ev.preventDefault();
        if (formRef.current) {
            const newSettings = {
                ...settings,
                ...serialize(formRef.current, { hash: true }),
            };

            if (JSON.stringify(newSettings) !== JSON.stringify(settings)) {
                setCurrentSettings(newSettings);
                onPaneLeave?.(true, newSettings, settings);
            } else {
                onPaneLeave?.(true, settings, settings);
            }
        }
    };

    const childrenWithProps = React.Children.map(children, (child) =>
    React.isValidElement<ChildProps>(child)
        ? React.cloneElement<ChildProps>(child as SettingsPaneChildElement, {
            items,
            settings: currentSettings,
            currentPage,
            onPaneLeave,
            onMenuItemClick,
            switchContent,
            onChange: settingsChanged,
        })
        : child
    );

    return (
        <div className={`settings-pane ${className}`}>
            <form ref={formRef} className="settings" onSubmit={handleSubmit}>
                {childrenWithProps}
            </form>
        </div>
    );
};

export default SettingsPane;
