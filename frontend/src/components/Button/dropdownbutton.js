import React, { useRef, useEffect } from 'react';
import './dropdownbutton.css';

const DropdownButton = ({ label, type, options, isOpen, setOpen, onOptionSelected }) => { // Added onOptionSelected
  const dropdownRef = useRef(null);

  // Handle clicks outside the dropdown to close it
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (isOpen && dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen, setOpen]);

  const handleButtonClick = () => {
    setOpen(!isOpen);
  };

  const handleOptionClick = (option) => {
    // Call the passed function
    if (onOptionSelected) {
      onOptionSelected(option);
    }
    // Optionally, show a simple alert for other options as before
    if (label !== "Profits" || option !== "Net Profit Margin") {
      //alert(`Selected ${label} option: ${option}`);
    }
    setOpen(false); // Close dropdown after selection
  };

  return (
    <div className={`dropdown-container ${isOpen ? 'open' : ''}`} ref={dropdownRef}>
      <button className={`dropdown-toggle dropdown-${type}`} onClick={handleButtonClick}>
        {label} <span className="dropdown-arrow">&#9662;</span>
      </button>
      {isOpen && (
        <div className="dropdown-menu">
          {options.map((option, index) => (
            <button
              key={index}
              className="dropdown-item"
              onClick={() => handleOptionClick(option)}
            >
              {option}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default DropdownButton;