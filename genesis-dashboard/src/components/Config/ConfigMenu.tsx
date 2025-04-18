import React from "react";

interface MenuItemType {
    url: string;
    title: string;
    name?: string;
}

interface ConfigMenuProps {
    headline: string;
    items: MenuItemType[];
    currentPage: string;
    switchContent: (menuItem: MenuItemType) => void;
    onMenuItemClick: (menuItem: MenuItemType) => void;
}

const ConfigMenu: React.FC<ConfigMenuProps> = ({
    headline,
    items,
    currentPage,
    switchContent,
    onMenuItemClick,
}) => {
    const handleClick = (menuItem: MenuItemType) => {
        onMenuItemClick(menuItem);
        switchContent(menuItem);
    };

    return (
        <div className="config-menu">
            <ul>
                {items.map((item, index) => (
                    <li
                        key={index}
                        className={`config-menu-item ${
                            currentPage === item.url ? "active" : ""
                        }`}
                        onClick={() => handleClick(item)}
                    >
                        {item.title}
                    </li>
                ))}
            </ul>
        </div>
    );
};

export default ConfigMenu;
