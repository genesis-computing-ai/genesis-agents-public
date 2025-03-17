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
    onChange?: (ev: React.ChangeEvent<HTMLFormElement>) => void;
    onPaneLeave?: (status: boolean, newConfig: any, oldConfig: any) => void;
    closeButtonClass?: string;
    saveButtonClass?: string;
}

interface ChildProps {
    config: Record<string, any>;
    currentPage: string;
    onPaneLeave?: (status: boolean, newConfig: any, oldConfig: any) => void;
    onChange: (ev: React.ChangeEvent<HTMLFormElement>) => void;
}

interface ConfigPaneChildElement extends React.ReactElement {
    props: ChildProps;
}

const ConfigPane: React.FC<ConfigPaneProps> = ({
    children,
    config,
    currentPage,
    className,
    onChange,
    onPaneLeave,
    closeButtonClass,
    saveButtonClass,
}) => {
    const [currentConfig, setCurrentConfig] = useState(config);
    const formRef = useRef<HTMLFormElement | null>(null);
    let keyUpListener: { remove: () => void } | null = null;

    const addEvent = (node: Document, event: string, handler: (ev: KeyboardEvent) => void) => {
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
            onPaneLeave?.(false, currentConfig, currentConfig);
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

    const configChanged = (ev: React.ChangeEvent<HTMLFormElement>) => {
        onChange?.(ev);
    };

    const handleSubmit = (ev: React.FormEvent<HTMLFormElement>) => {
        ev.preventDefault();
        if (formRef.current) {
            const newConfig = {
                ...config,
                ...serialize(formRef.current, { hash: true }),
            };

            if (JSON.stringify(newConfig) !== JSON.stringify(config)) {
                setCurrentConfig(newConfig);
                onPaneLeave?.(true, newConfig, config);
            } else {
                onPaneLeave?.(true, config, config);
            }
        }
    };

    const childrenWithProps = React.Children.map(children, (child) =>
        React.isValidElement<ChildProps>(child)
            ? React.cloneElement<ChildProps>(child as ConfigPaneChildElement, {
                config: currentConfig,
                currentPage,
                onPaneLeave,
                onChange: configChanged,
            })
            : child
    );

    return (
        <div className={`config-pane ${className}`}>
            <form ref={formRef} className="configs" onSubmit={handleSubmit}>
                {childrenWithProps}
            </form>
        </div>
    );
};

export default ConfigPane;
