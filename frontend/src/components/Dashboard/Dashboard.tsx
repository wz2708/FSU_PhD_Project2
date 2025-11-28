import React, { useState, useEffect } from 'react';
import VegaLiteChart from './VegaLiteChart';
import './Dashboard.css';

interface ChartData {
  id: string;
  spec: any;
  title: string | { text?: string; anchor?: string; fontSize?: number };
  timestamp: Date;
}

const Dashboard: React.FC = () => {
  const [charts, setCharts] = useState<ChartData[]>([]);

  useEffect(() => {
    const handleNewChart = (event: CustomEvent) => {
      const chartSpec = event.detail;
      if (chartSpec) {
        let actualSpec = chartSpec;
        if (chartSpec.spec && typeof chartSpec.spec === 'object') {
          actualSpec = chartSpec.spec;
        }
        
        // Extract title safely - handle both string and object formats
        // CRITICAL: Always ensure title is a string, never an object
        let chartTitle: string = 'Chart';
        
        // Try description first
        if (actualSpec.description) {
          if (typeof actualSpec.description === 'string') {
            chartTitle = actualSpec.description;
          }
        }
        // Then try title
        else if (actualSpec.title) {
          if (typeof actualSpec.title === 'string') {
            chartTitle = actualSpec.title;
          } else if (typeof actualSpec.title === 'object' && actualSpec.title !== null) {
            // Handle Vega-Lite title object format {anchor, fontSize, text}
            if ('text' in actualSpec.title && typeof actualSpec.title.text === 'string') {
              chartTitle = actualSpec.title.text;
            }
          }
        }
        // Fallback to chartSpec.title
        else if (chartSpec.title) {
          if (typeof chartSpec.title === 'string') {
            chartTitle = chartSpec.title;
          } else if (typeof chartSpec.title === 'object' && chartSpec.title !== null && 'text' in chartSpec.title) {
            chartTitle = String(chartSpec.title.text || 'Chart');
          }
        }
        
        if (typeof chartTitle !== 'string') {
          chartTitle = String(chartTitle) || 'Chart';
        }
        
        const newChart: ChartData = {
          id: Date.now().toString(),
          spec: actualSpec,
          title: chartTitle,
          timestamp: new Date(),
        };
        
        setCharts((prev) => [...prev, newChart]);
      }
    };

    window.addEventListener('newChart' as any, handleNewChart as EventListener);

    return () => {
      window.removeEventListener('newChart' as any, handleNewChart as EventListener);
    };
  }, []);

  const removeChart = (id: string) => {
    setCharts((prev) => prev.filter((chart) => chart.id !== id));
  };

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Visualizations</h2>
        {charts.length > 0 && (
          <button onClick={() => setCharts([])} className="clear-all">
            Clear All
          </button>
        )}
      </div>
      <div className="dashboard-content">
        {charts.length === 0 ? (
          <div className="dashboard-empty">
            <p>No visualizations yet. Ask a question to generate charts!</p>
          </div>
        ) : (
          charts.map((chart) => {
            // Ensure title is always a string
            let displayTitle = 'Chart';
            if (typeof chart.title === 'string') {
              displayTitle = chart.title;
            } else if (chart.title && typeof chart.title === 'object' && 'text' in chart.title) {
              displayTitle = chart.title.text || 'Chart';
            }
            
            return (
              <div key={chart.id} className="chart-container">
                <div className="chart-header">
                  <h3>{displayTitle}</h3>
                  <button onClick={() => removeChart(chart.id)} className="remove-chart">
                    ×
                  </button>
                </div>
                <VegaLiteChart spec={chart.spec} />
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};

export default Dashboard;

