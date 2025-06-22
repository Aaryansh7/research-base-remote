// src/features/Profitability/OperatingMarginChart.js
import React, { useState, useEffect } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { fetchOperatingMargin } from '../../api/ratios';
import { formatPercentage } from '../../utils/formatters'; // Using the formatter
// Assuming you create a LoadingSpinner component
// import LoadingSpinner from '../../components/LoadingSpinner/LoadingSpinner';


const OperatingMarginChart = ({ selectedCompanyTicker, selectedCompanyName }) => {
  const [graphData, setGraphData] = useState(null);
  const [stats, setStats] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadData = async () => {
      if (!selectedCompanyTicker) {
        // setError("No company selected. Please select a company from the search bar.");
        return; // Don't fetch if no company is selected
      }

      setIsLoading(true);
      setError(null);
      setGraphData(null);
      setStats(null);

      try {
        const result = await fetchOperatingMargin(selectedCompanyTicker); // Call the centralized API function

        if (result.status === "success" && result.data) {
          setGraphData(result.data.graph_data);
          console.log(result.data.graph_data);
          setStats(result.data.statistics);
        } else {
          setError(result.message || "Failed to load operating margin data.");
        }
      } catch (err) {
        console.error("Failed to fetch Operating Margin data:", err);
        setError(`Failed to load Operating Margin: ${err.message || 'Network error'}.`);
      } finally {
        setIsLoading(false);
      }
    };

    // This effect runs whenever selectedCompanyTicker changes,
    // ensuring data is re-fetched for a new company.
    loadData();
  }, [selectedCompanyTicker]); // Dependency array: re-run when selectedCompanyTicker changes

  if (isLoading) {
    return (
      <div className="stats-box loading">
        {/* <LoadingSpinner /> */}
        <div className="spinner"></div>
        <p>Loading Operating Margin data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="stats-box error">
        <p>Error: {error}</p>
        <p>Ensure backend is running and data for {selectedCompanyName || 'the selected company'} is processed.</p>
      </div>
    );
  }

  //console.log(graphData);

  if (!graphData || graphData.length === 0) {
    return (
      <div className="stats-box">
        <p>No Operating Margin data available for {selectedCompanyName || 'this company'}.</p>
      </div>
    );
  }

  return (
    <div className="stats-box">
      <h2>Operating Margin Trend for {selectedCompanyName || 'Selected Company'}</h2>
      <div style={{ width: '100%', height: 300 }}>
        <ResponsiveContainer>
          <LineChart
            data={graphData}
            margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis tickFormatter={formatPercentage} /> {/* Use the formatter for Y-axis */}
            <Tooltip formatter={(value) => formatPercentage(value)} /> {/* Format tooltip values */}
            <Legend />
            <Line type="monotone" dataKey="value" stroke="#8884d8" activeDot={{ r: 8 }} name="Operating Margin (%)" />
          </LineChart>
        </ResponsiveContainer>
      </div>
      {stats && (
        <div className="revenue-statistics mt-4 p-4 bg-gray-100 rounded-md shadow-inner">
          <h3 className="text-lg font-semibold text-gray-700 mb-2">Statistics:</h3>
          <p className="text-gray-600"><strong>Average Operating Margin:</strong> {formatPercentage(stats.average_margin)}</p>
          <p className="text-gray-600"><strong>Standard Deviation Operating Margin:</strong> {formatPercentage(stats.std_dev_margin)}</p>
          <p className="text-sm text-gray-500 mt-2">
            Note: This graph displays the Operating Margin ratio over available periods.
          </p>
        </div>
      )}
    </div>
  );
};

export default OperatingMarginChart;