// src/api/companyInfo.js
const BASE_URL = 'http://127.0.0.1:5000';

export const fetchCompanyInfo = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/company-info/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};