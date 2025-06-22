// src/api/ratiosApi.js
const BASE_URL = 'http://127.0.0.1:5000';

export const fetchNetProfitMargin = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/profitability/net-margin/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchOperatingMargin = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/profitability/operating-margin/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchCurrentRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/liquidity/current-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchCashRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/liquidity/cash-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchDebtEquityRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/solvency/debtequity-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchDebtAssetRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/solvency/debtasset-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchInventoryTurnoverRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/efficiency/inventoryturnover-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};

export const fetchAssetTurnoverRatio = async (ticker) => {
    const response = await fetch(`${BASE_URL}/api/efficiency/assetturnover-ratio/${ticker}`);
    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorData.message}`);
    }
    return response.json();
};
// export const fetchOperatingProfit = async () => { ... };
// export const fetchCurrentRatio = async () => { ... };