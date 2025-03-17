import React from "react";
import MenuItem from "./MenuItem";

interface MenuItemType {
  url: string;
  name?: string;
  title: string;
  [key: string]: any;
}

interface SettingsMenuProps {
  headline: string;
  items: MenuItemType[];
  currentPage: string;
  switchContent: (menuItem: MenuItemType) => void;
  onMenuItemClick: (menuItem: MenuItemType) => void;
}

const SettingsMenu: React.FC<SettingsMenuProps> = ({
  headline,
  items,
  currentPage,
  switchContent,
  onMenuItemClick,
}) => {
  const menuItems = () => {
    return items.map((item, i) => (
      <MenuItem
        key={i}
        item={item}
        active={currentPage === item.url}
        onMenuItemClick={onMenuItemClick}
        switchContent={switchContent}
      />
    ));
  };

  return (
    <div className="settings-left">
      <ul className="settings-menu">
        {headline ? <li className="headline">{headline}</li> : ""}
        {menuItems()}
      </ul>
    </div>
  );
};

export default SettingsMenu;
