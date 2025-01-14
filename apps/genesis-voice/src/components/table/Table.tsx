import React from 'react';
import './Table.scss';

interface TableProps {
  data: any[];
}

const renderCell = (value: any): React.ReactNode => {
  if (typeof value === 'object' && value !== null) {
    return JSON.stringify(value);
  }
  return value;
};

export const Table: React.FC<TableProps> = ({ data }) => {
  console.log("Table data:", data);

  if (!data || data.length === 0) {
    return <p>No data available</p>;
  }

  // Get the column names in the order they appear in the first row
  const headers = Object.keys(data[0]);

  return (
    <div className="table-container">
      <table className="data-table">
        <thead>
          <tr>
            {headers.map((header, index) => (
              <th key={index}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {headers.map((header, cellIndex) => (
                <td key={cellIndex}>{renderCell(row[header])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};