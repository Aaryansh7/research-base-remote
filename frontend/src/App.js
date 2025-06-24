import React, { useEffect, useState } from 'react';
import DropdownButton from './components/Button/dropdownbutton';
import './App.css';

// Import API functions
import { fetchCompanyInfo } from './api/companyInfo';

// Import ratio components
import NetProfitMarginChart from './features/ProfitabilityRatios/NetProfitMargin';
import OperatingMarginChart from './features/ProfitabilityRatios/OperatingMargin';
import CurrentRatioChart from './features/LiquidityRatios/CurrentRatio';
import CashRatioChart from './features/LiquidityRatios/CashRatio';
import DebtEquityRatioChart from './features/SolvencyRatios/DebtEquityRatio';
import DebtAssetRatioChart from './features/SolvencyRatios/DebtAssetRatio';
import InventoryTurnoverChart from './features/EfficiencyRatios/InventoryTurnover';
import AssetTurnoverChart from './features/EfficiencyRatios/AssetTurnover';

// You will import other ratio components here as you create them:
// import GrossProfitChart from './features/Profitability/GrossProfitChart';


function App() {
  const [openDropdownIndex, setOpenDropdownIndex] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  // --- Search Functionality State ---
  const [searchQuery, setSearchQuery] = useState('');
  const [allCompanies] = useState([
    {"name": "Apple Inc.", "ticker": "AAPL"},
    {"name": "Microsoft Corp.", "ticker": "MSFT"},
    {"name": "Amazon.com Inc.", "ticker": "AMZN"},
    {"name": "Alphabet Inc.", "ticker": "GOOG"},
    {"name": "NVIDIA Corp.", "ticker": "NVDA"},
    {"name": "Meta Platforms Inc.", "ticker": "META"},
    {"name": "Tesla Inc.", "ticker": "TSLA"},
    {"name": "Berkshire Hathaway Inc. (Class B)", "ticker": "BRK.B"},
    {"name": "Eli Lilly and Company", "ticker": "LLY"},
    {"name": "Johnson & Johnson", "ticker": "JNJ"},
    {"name": "Visa Inc.", "ticker": "V"},
    {"name": "JPMorgan Chase & Co.", "ticker": "JPM"},
    {"name": "Walmart Inc.", "ticker": "WMT"},
    {"name": "Exxon Mobil Corp.", "ticker": "XOM"},
    {"name": "UnitedHealth Group Inc.", "ticker": "UNH"},
    {"name": "Taiwan Semiconductor Manufacturing Company Limited (ADR)", "ticker": "TSM"},
    {"name": "Procter & Gamble Co.", "ticker": "PG"},
    {"name": "Broadcom Inc.", "ticker": "AVGO"},
    {"name": "Chevron Corp.", "ticker": "CVX"},
    {"name": "Merck & Co. Inc.", "ticker": "MRK"},
    {"name": "Coca-Cola Co.", "ticker": "KO"},
    {"name": "PepsiCo Inc.", "ticker": "PEP"},
    {"name": "Intel Corp.", "ticker": "INTC"},
    {"name": "Salesforce Inc.", "ticker": "CRM"},
    {"name": "Bank of America Corp.", "ticker": "BAC"},
    {"name": "AbbVie Inc.", "ticker": "ABBV"},
    {"name": "Adobe Inc.", "ticker": "ADBE"},
    {"name": "Costco Wholesale Corp.", "ticker": "COST"},
    {"name": "McDonald's Corporation", "ticker": "MCD"},
    {"name": "Boeing Co.", "ticker": "BA"},
    {"name": "Home Depot Inc.", "ticker": "HD"},
    {"name": "Netflix Inc.", "ticker": "NFLX"}
  ]);
  const [filteredCompanies, setFilteredCompanies] = useState([]);
  const [showSearchResults, setShowSearchResults] = useState(false);
  const [selectedCompanyTicker, setSelectedCompanyTicker] = useState(null);
  const [selectedCompanyName, setSelectedCompanyName] = useState(null);
  // State to determine which ratio chart to display
  const [activeRatioComponent, setActiveRatioComponent] = useState(null);
  // --- End Search State ---

  const dropdownsData = [
    { label: "Profitability", type: "primary", options: ['Net Profit Margin', 'Operating Margin'] },
    { label: "Liquidity", type: "secondary", options: ['Current Ratio', 'Cash Ratio'] },
    { label: "Solvency", type: "success", options: ['Debt to Equity Ratio', 'Debt to Asset Ratio'] },
    { label: "Efficiency", type: "info", options: ['Inventory Turnover', 'Asset Turnover'] },
    { label: "Valuation", type: "warning", options: ['P/E Ratio', 'P/B Ratio'] },
  ];

  // --- useEffect for Search Filtering ---
  useEffect(() => {
    if (searchQuery.length > 0) {
      const filtered = allCompanies.filter(company =>
        company.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        company.ticker.toLowerCase().includes(searchQuery.toLowerCase())
      );
      setFilteredCompanies(filtered);
    } else {
      setFilteredCompanies([]);
    }
  }, [searchQuery, allCompanies]);

  // --- Search Input Handlers ---
  const handleSearchChange = (event) => {
    setSearchQuery(event.target.value);
    setShowSearchResults(event.target.value.length > 0);
    setSelectedCompanyTicker(null);
    setSelectedCompanyName(null);
    setActiveRatioComponent(null); // Clear displayed ratio when search changes
    setError(null);
  };

  const handleSearchFocus = () => {
    if (searchQuery.length > 0) {
      setShowSearchResults(true);
    }
  };

  const handleResultMouseDown = (event) => {
    event.preventDefault();
  };

  const handleCompanySelect = async (company) => {
    //alert(`You selected: ${company.name} (${company.ticker})`);
    setSearchQuery(company.name);
    setFilteredCompanies([]);
    setShowSearchResults(false);
    setSelectedCompanyTicker(company.ticker);
    setSelectedCompanyName(company.name);
    setActiveRatioComponent(null); // Clear any active ratio display

    console.log(`Company "${company.name}" selected from search. Now fetching overview data.`);

    setIsLoading(true);
    setError(null);
    try {
        const data = await fetchCompanyInfo(company.ticker); // Use the API function
        console.log("Company info API response:", data);
        //alert(data.message || `Data for ${company.name} processed successfully!`);

    } catch (err) {
        console.error(`Failed to fetch and process info for ${company.ticker}:`, err);
        setError(`Failed to process company info for ${company.ticker}. ${err.message || ''}`);
    } finally {
        setIsLoading(false);
    }
  };
  // --- End Search Input Handlers ---

  // --- Handle option selection from dropdowns ---
  const handleDropdownOptionSelected = (dropdownLabel, optionLabel) => {
    setOpenDropdownIndex(null); // Close the dropdown

    if (!selectedCompanyTicker) {
        alert("Please select a company first from the search bar.");
        setError("No company selected.");
        return;
    }

    setError(null); // Clear previous errors

    // Logic to set the active ratio component based on selected option
    switch (optionLabel) {
        case 'Net Profit Margin':
            setActiveRatioComponent('NetProfitMargin');
            break;
        case 'Operating Margin':
            setActiveRatioComponent('OperatingMargin');
            break;
        // Add cases for other ratios here
        case 'Current Ratio':
            setActiveRatioComponent('CurrentRatio');
            break;
        case 'Cash Ratio':
            setActiveRatioComponent('CashRatio');
            //setError("Quick Ratio feature not yet implemented.");
            break;

        case 'Debt to Equity Ratio':
            setActiveRatioComponent('DebtEquityRatio');
            //setError("Quick Ratio feature not yet implemented.");
            break;
        case 'Debt to Asset Ratio':
            setActiveRatioComponent('DebtAssetRatio');
            //setError("Quick Ratio feature not yet implemented.");
            break;
        case 'Inventory Turnover':
            setActiveRatioComponent('InventoryTurnover');
            //setError("Quick Ratio feature not yet implemented.");
            break;
        case 'Asset Turnover':
            setActiveRatioComponent('AssetTurnover');
            //setError("Quick Ratio feature not yet implemented.");
            break;
        default:
            setActiveRatioComponent(null);
            setError(`'${optionLabel}' not yet implemented.`);
            break;
    }
  };

  // Render content box based on loading, error, or active ratio component
  const renderContentBox = () => {
    if (isLoading) {
      return (
        <div className="stats-box loading">
          <div className="spinner"></div>
          <p>Loading data...</p>
        </div>
      );
    }

    if (error) {
      return (
        <div className="stats-box error">
          <p>Error: {error}</p>
          <p>Please ensure the backend is running and a company's data has been processed, and that file paths are correct.</p>
        </div>
      );
    }

    if (!selectedCompanyTicker) {
      return (
        <div className="stats-box">
          <p>Please search and select a company to view its financial data.</p>
        </div>
      );
    }

    // Render the active ratio component based on the state
    switch (activeRatioComponent) {
        case 'NetProfitMargin':
            return (
                <NetProfitMarginChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );
        case 'OperatingMargin':
            return (
                <OperatingMarginChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );
        case 'CurrentRatio':
            return (
                <CurrentRatioChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );

        case 'CashRatio':
            return (
                <CashRatioChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );

        case 'DebtEquityRatio':
            return (
                <DebtEquityRatioChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );

        case 'DebtAssetRatio':
            return (
                <DebtAssetRatioChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );

        case 'InventoryTurnover':
            return (
                <InventoryTurnoverChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );

        case 'AssetTurnover':
            return (
                <AssetTurnoverChart
                    selectedCompanyTicker={selectedCompanyTicker}
                    selectedCompanyName={selectedCompanyName}
                />
            );
        // Add cases for other ratio components here
        // case 'GrossProfit':
        //     return <GrossProfitChart selectedCompanyTicker={selectedCompanyTicker} selectedCompanyName={selectedCompanyName} />;
        // case 'CurrentRatio':
        //     return <CurrentRatioChart selectedCompanyTicker={selectedCompanyTicker} selectedCompanyName={selectedCompanyName} />;
        default:
            return (
                <div className="stats-box">
                    <p>Data ready for {selectedCompanyName}. Please select a metric to view details.</p>
                </div>
            );
    }
  };


  return (
    <div className="App">
      <header className="App-header">
        <h1>Financial Dashboard</h1>
      </header>
      <main className="App-main">
        <div className="search-container">
          <input
            type="text"
            className="search-input"
            placeholder="Search US Public Companies (e.g., Apple)"
            value={searchQuery}
            onChange={handleSearchChange}
            onFocus={handleSearchFocus}
            onBlur={() => setTimeout(() => setShowSearchResults(false), 200)}
          />
          {showSearchResults && searchQuery.length > 0 && (
            <div className="search-results-dropdown-wrapper">
              {filteredCompanies.length > 0 ? (
                <ul className="search-results">
                  {filteredCompanies.slice(0, 10).map((company) => (
                    <li
                      key={company.ticker}
                      onClick={() => handleCompanySelect(company)}
                      onMouseDown={handleResultMouseDown}
                    >
                      {company.name} ({company.ticker})
                    </li>
                  ))}
                </ul>
              ) : (
                <div className="no-results">No companies found matching "{searchQuery}"</div>
              )}
            </div>
          )}
        </div>

        {selectedCompanyName && (
            <div className="selected-company-display">
                Currently analyzing: <strong>{selectedCompanyName} ({selectedCompanyTicker})</strong>
            </div>
        )}

        <div className="button-row">
          {dropdownsData.map((dropdown, index) => (
            <DropdownButton
              key={index}
              label={dropdown.label}
              type={dropdown.type}
              options={dropdown.options}
              isOpen={openDropdownIndex === index}
              setOpen={(isOpen) => setOpenDropdownIndex(isOpen ? index : null)}
              onOptionSelected={(optionLabel) => handleDropdownOptionSelected(dropdown.label, optionLabel)}
            />
          ))}
        </div>
        {renderContentBox()}
      </main>
    </div>
  );
}

export default App;