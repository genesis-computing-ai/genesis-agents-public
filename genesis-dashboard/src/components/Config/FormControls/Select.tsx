import React from "react";

interface SelectProps {
    name: string;
    value?: string;
    className?: string;
}

const Select: React.FC<SelectProps> = ({ name, value, className }) => {
    return (
        <input
            type="checkbox"
            className={className}
            name={name}
            value={value}
        />
    );
};

export default Select;
