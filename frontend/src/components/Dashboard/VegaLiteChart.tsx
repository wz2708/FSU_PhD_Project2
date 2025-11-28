import React, { useEffect, useRef, useState } from 'react';
import * as vegaEmbed from 'vega-embed';

interface VegaLiteChartProps {
  spec: any;
}

const VegaLiteChart: React.FC<VegaLiteChartProps> = ({ spec }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<any>(null);
  const [containerSize, setContainerSize] = useState({ width: 0, height: 0 });

  // Observe container size changes
  useEffect(() => {
    if (!containerRef.current) return;

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        setContainerSize({ width, height });
      }
    });

    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
    };
  }, []);

  useEffect(() => {
    if (chartRef.current && spec && containerSize.width > 0) {
      if (viewRef.current) {
        try {
          viewRef.current.finalize();
        } catch (e) {
          // Ignore finalization errors
        }
      }
      
      chartRef.current.innerHTML = '';
      
      // Deep clone spec to preserve selection structure
      const responsiveSpec = JSON.parse(JSON.stringify(spec));
      
      // Update dimensions
      responsiveSpec.width = containerSize.width - 40 || spec.width || 600;
      responsiveSpec.height = Math.min(containerSize.height - 100 || spec.height || 400, 600);
      
      // Only add autosize if selection is not present (autosize can conflict with selections)
      // If selection exists, rely on manual width/height setting
      if (!responsiveSpec.selection && !responsiveSpec.autosize) {
        responsiveSpec.autosize = {
          type: 'fit',
          contains: 'padding'
        };
      }
      
      vegaEmbed.default(chartRef.current, responsiveSpec, {
        actions: false,
        renderer: 'svg'
      })
      .then((result: any) => {
        viewRef.current = result.view;
        
        // Resize view to fit container
        if (result.view && containerSize.width > 0) {
          result.view.resize();
        }
        
      })
      .catch((error: any) => {
        console.error('[VegaLiteChart] Error rendering chart:', error);
        if (chartRef.current) {
          chartRef.current.innerHTML = `
            <div style="padding: 20px; color: #dc3545; text-align: center;">
              <p>Error rendering chart</p>
              <pre style="font-size: 12px; text-align: left; background: #f8f8f8; padding: 10px; border-radius: 4px;">${error.message || error}</pre>
            </div>
          `;
        }
      });
    }
    
    return () => {
      if (viewRef.current) {
        try {
          viewRef.current.finalize();
        } catch (e) {
          // Ignore cleanup errors
        }
      }
    };
  }, [spec, containerSize]);

  if (!spec) {
    return (
      <div style={{ padding: '40px', textAlign: 'center', color: '#999' }}>
        No chart specification provided
      </div>
    );
  }

  return (
    <div ref={containerRef} className="vega-lite-chart-container" style={{ width: '100%', height: '100%', minHeight: '400px' }}>
      <div ref={chartRef} className="vega-lite-chart" style={{ width: '100%', height: '100%' }} />
    </div>
  );
};

export default VegaLiteChart;
