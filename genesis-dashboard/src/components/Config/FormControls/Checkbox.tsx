import React, { FC } from "react";

interface CheckboxProps {
  name: string;
  value?: string;
  className?: string;
}

const Checkbox: FC<CheckboxProps> = ({ name, value, className }) => {
  return (
    <input type="checkbox" className={className} name={name} value={value} />
  );
};

export default Checkbox;
