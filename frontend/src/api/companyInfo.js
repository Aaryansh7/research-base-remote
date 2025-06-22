// src/api/companyInfo.js
const BASE_URL = process.env.REACT_APP_API_BASE_URL;

export const fetchCompanyInfo = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/company-info/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};