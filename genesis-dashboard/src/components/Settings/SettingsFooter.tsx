import React from "react";

interface SettingsFooterProps {
  settings: object;
  onPaneLeave?: (
    status: boolean,
    newSettings: object,
    oldSettings: object
  ) => void;
  closeButtonClass?: string;
  saveButtonClass?: string;
}

const SettingsFooter: React.FC<SettingsFooterProps> = ({
  settings,
  onPaneLeave,
  closeButtonClass,
  saveButtonClass,
}) => {
  const closeClicked = (ev: React.MouseEvent<HTMLButtonElement>) => {
    ev.preventDefault();
    onPaneLeave?.(false, settings, settings); // Added optional chaining
  };

  const saveClicked = (ev: React.MouseEvent<HTMLButtonElement>) => {
    ev.preventDefault();
    onPaneLeave?.(true, settings, settings); // Added optional chaining
  };

  return (
    <div className="settings-footer">
      <div className="settings-save">
        <button
          className={closeButtonClass || "btn btn-default"}
          onClick={closeClicked}
        >
          Save
        </button>
      </div>
    </div>
  );
};

export default SettingsFooter;
