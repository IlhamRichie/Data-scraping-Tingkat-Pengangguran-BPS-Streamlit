import pandas as pd

def format_date(date_str):
    """
    Format date from BPS API to YYYY-MM-DD
    """
    try:
        # Handle different date formats from BPS
        if '-' in date_str:
            year, month = date_str.split('-')
            return f"{year}-{month}-01"
        elif len(date_str) == 4:  # Just year
            return f"{date_str}-01-01"
        else:  # Handle other cases
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
    except:
        return date_str  # Return original if parsing fails