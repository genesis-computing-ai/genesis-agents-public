import React, { FC } from "react";

interface InputProps {
    name: string;
    type?: string;
    value?: string;
    className?: string;
}

const Input: FC<InputProps> = ({ name, type = "text", value, className }) => {
    return (
        <input
            type={type}
            className={className}
            name={name}
            value={value}
        />
    );
};

export default Input;
