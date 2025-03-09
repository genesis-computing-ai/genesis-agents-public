import { FC, MouseEvent } from "react";

interface MenuItemType {
  url: string;
  title: string;
  name?: string;
}

interface MenuItemProps {
  item: MenuItemType;
  active?: boolean;
  switchContent: (item: MenuItemType) => void;
  onMenuItemClick?: (item: MenuItemType) => void;
}

const MenuItem: FC<MenuItemProps> = ({
  item,
  switchContent,
  onMenuItemClick,
  active,
}) => {
  const clicked = (ev: MouseEvent<HTMLLIElement>) => {
    ev.preventDefault();

    if (ev.button !== 0) {
      return;
    }

    // Ensure item has required properties
    const menuItem: MenuItemType = {
      url: item.url,
      title: item.title || item.name || item.url,
    };

    onMenuItemClick?.(menuItem);
    switchContent(menuItem);
  };

  return (
    <li
      className={`menu-item ${active ? "active" : ""}`}
      onClick={clicked}
      role="button"
      tabIndex={0}
      style={{ cursor: 'pointer' }}
    >
      {item.title || item.name || item.url}
    </li>
  );
};

export default MenuItem;
