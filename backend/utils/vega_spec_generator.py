"""
Vega-Lite specification generator utilities.
"""

from typing import Dict, List, Any, Optional
import uuid


def create_bar_chart(data: List[Dict], x_field: str, y_field: str, 
                     title: str = "", color: Optional[str] = None) -> Dict:
    """Create an interactive bar chart Vega-Lite specification with tooltip, selection, and filtering."""
    if not data:
        raise ValueError("Data cannot be empty")
    
    x_type = "nominal" if isinstance(data[0].get(x_field), str) else "quantitative"
    x_title = x_field.replace("_", " ").title()
    y_title = y_field.replace("_", " ").title()
    
    # Generate unique selection names to avoid conflicts when multiple charts are rendered
    unique_id = str(uuid.uuid4())[:8]
    brush_name = f"brush_{unique_id}"
    click_name = f"click_{unique_id}"
    
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": title,
        "data": {"values": data},
        # Selection for filtering - must be defined before transform
        "selection": {
            brush_name: {
                "type": "interval",
                "encodings": ["x"]
            },
            click_name: {
                "type": "point",
                "on": "click",
                "toggle": True,
                "nearest": True
            }
        },
        # Transform to actually filter data based on selection
        "transform": [
            {
                "filter": {
                    "selection": brush_name
                }
            }
        ],
        "mark": {
            "type": "bar",
            "cursor": "pointer",
            "tooltip": True
        },
        "encoding": {
            "x": {
                "field": x_field,
                "type": x_type,
                "title": x_title,
                "axis": {"labelAngle": -45 if x_type == "nominal" else 0}
            },
            "y": {
                "field": y_field,
                "type": "quantitative",
                "title": y_title
            },
            # Tooltip for hovering interaction
            "tooltip": [
                {"field": x_field, "type": x_type, "title": x_title},
                {"field": y_field, "type": "quantitative", "title": y_title, "format": ",.0f"}
            ]
        },
        "width": 600,
        "height": 400
    }
    
    # Color encoding with conditional highlighting based on selection
    if color:
        spec["encoding"]["color"] = {
            "condition": {
                "selection": brush_name,
                "value": color
            },
            "value": "lightgray"
        }
    else:
        spec["encoding"]["color"] = {
            "condition": {
                "selection": brush_name,
                "value": "#4A90E2"
            },
            "value": "lightgray"
        }
    
    return spec


def create_line_chart(data: List[Dict], x_field: str, y_field: str,
                      title: str = "") -> Dict:
    """Create an interactive line chart Vega-Lite specification with tooltip, selection, and filtering."""
    if not data:
        raise ValueError("Data cannot be empty")
    
    x_title = x_field.replace("_", " ").title()
    y_title = y_field.replace("_", " ").title()
    
    # Generate unique selection names to avoid conflicts
    unique_id = str(uuid.uuid4())[:8]
    brush_name = f"brush_{unique_id}"
    click_name = f"click_{unique_id}"
    
    # Line chart with filtering capability
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": title,
        "data": {"values": data},
        # Selection for filtering - must be defined before transform
        "selection": {
            brush_name: {
                "type": "interval",
                "encodings": ["x"]
            },
            click_name: {
                "type": "point",
                "on": "click",
                "nearest": True
            }
        },
        # Transform to actually filter data based on selection
        "transform": [
            {
                "filter": {
                    "selection": brush_name
                }
            }
        ],
        "mark": {
            "type": "line",
            "stroke": "#4A90E2",
            "strokeWidth": 2,
            "point": {
                "filled": True,
                "size": 80,
                "color": "#4A90E2",
                "cursor": "pointer"
            }
        },
        "encoding": {
            "x": {
                "field": x_field,
                "type": "quantitative",
                "title": x_title,
                "scale": {"nice": True}
            },
            "y": {
                "field": y_field,
                "type": "quantitative",
                "title": y_title,
                "scale": {"nice": True}
            },
            "tooltip": [
                {"field": x_field, "type": "quantitative", "title": x_title, "format": ",.0f"},
                {"field": y_field, "type": "quantitative", "title": y_title, "format": ",.0f"}
            ]
        },
        "width": 600,
        "height": 400
    }
    
    return spec


def create_histogram(data: List[Dict], field: str, title: str = "") -> Dict:
    """Create an interactive histogram Vega-Lite specification with tooltip, selection, and filtering."""
    if not data:
        raise ValueError("Data cannot be empty")
    
    field_title = field.replace("_", " ").title()
    
    # Generate unique selection names to avoid conflicts
    unique_id = str(uuid.uuid4())[:8]
    brush_name = f"brush_{unique_id}"
    click_name = f"click_{unique_id}"
    
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": title,
        "data": {"values": data},
        # Selection for filtering - must be defined before transform
        "selection": {
            brush_name: {
                "type": "interval",
                "encodings": ["x"]
            },
            click_name: {
                "type": "point",
                "on": "click",
                "toggle": True,
                "nearest": True
            }
        },
        # Transform to actually filter data based on selection
        "transform": [
            {
                "filter": {
                    "selection": brush_name
                }
            }
        ],
        "mark": {
            "type": "bar",
            "cursor": "pointer",
            "tooltip": True
        },
        "encoding": {
            "x": {
                "field": field,
                "type": "quantitative",
                "bin": {
                    "maxbins": 30,
                    "step": 1
                },
                "title": field_title
            },
            "y": {
                "aggregate": "count",
                "type": "quantitative",
                "title": "Count"
            },
            "color": {
                "value": "#4A90E2"
            },
            # Tooltip for hovering interaction
            "tooltip": [
                {
                    "field": field,
                    "type": "quantitative",
                    "bin": True,
                    "title": field_title,
                    "format": ",.0f"
                },
                {
                    "aggregate": "count",
                    "type": "quantitative",
                    "title": "Count",
                    "format": ",.0f"
                }
            ]
        },
        "width": 600,
        "height": 400
    }

