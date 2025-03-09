import React, { ReactNode } from "react";
import { SettingsFooter } from "./index";

interface SettingsContentProps {
  currentPage?: string;
  header?: boolean | ReactNode;
  items?: { url: string; title: string }[];
  settings?: object;
  onChange?: (value: any) => void;
  switchContent?: (url: string) => void;
  onPaneLeave?: (
    status: boolean,
    newSettings: object,
    oldSettings: object
  ) => void;
  onMenuItemClick?: (url: string) => void;
  children?: ReactNode;
  closeButtonClass?: string;
  saveButtonClass?: string;
}

const SettingsContent: React.FC<SettingsContentProps> = (props) => {
    const renderPage = (url: string | undefined) => {
        let page: ReactNode[] = [];

        if (url) {
            // Search for a matching url handler
            page = React.Children.map(props.children, (child: any) => {
                if (child.props.handler && child.props.handler === url) {
                    return React.cloneElement(child, {
                        settings: props.settings,
                        onChange: props.onChange,
                        onPaneLeave: props.onPaneLeave,
                        onMenuItemClick: props.onMenuItemClick,
                        currentPage: props.currentPage,
                    });
                }
            }) || [];
        }

        // There was no page found, so show a page not defined message
        if (page.length === 0) {
            page = [
                <div key="settingsEmptyMessage" className="empty-message">
                    <p>Under Construction</p>
                </div>,
            ];
        }

        return page;
    };

    const page = props.currentPage || "";
    let header: ReactNode = "";

    if (props.header) {
        if (props.header === true) {
            const currentItem = props.items?.reduce(
                (prev, item) => (item.url === page ? item : prev),
                { url: "", title: "" }
            );
            header = (
                <div className="headline">
                    <h3>{currentItem?.title}</h3>
                </div>
            );
        } else {
            header = props.header;
        }
    }

    return (
      <div className="settings-content">
        {header}
        <div className="settings-page">
          <div className="scroller-wrap">{renderPage(page)}</div>
        </div>

        <SettingsFooter
          settings={props.settings || {}}
          onPaneLeave={props.onPaneLeave}
          closeButtonClass={props.closeButtonClass}
          saveButtonClass={props.saveButtonClass}
        />
      </div>
    );
};

export default SettingsContent;
