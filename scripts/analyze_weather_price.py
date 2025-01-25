import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import select, func
from pathlib import Path
import sys
import logging
from datetime import datetime

# Add the parent directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from app.db.models import WeatherData, FuturesData
from app.db.database import engine

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_combined_data() -> pd.DataFrame:
    """Fetch and combine weather and futures data."""
    try:
        # Create the query using SQLAlchemy ORM
        query = (
            select(
                WeatherData.date,
                WeatherData.high_temp,
                WeatherData.low_temp,
                WeatherData.avg_temp,
                WeatherData.cdd,
                WeatherData.hdd,
                FuturesData.close.label('price'),
                FuturesData.symbol
            )
            .join(
                FuturesData,
                func.date(FuturesData.timestamp) == WeatherData.date
            )
            .where(
                # Filter for front-month contracts
                FuturesData.symbol.regexp_match('^HH[A-Z]\d$')
            )
            .where(
                # Exclude spread products
                ~FuturesData.symbol.like('%-%')
            )
            .order_by(WeatherData.date)
        )

        # Execute query and convert to DataFrame
        with engine.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
        logger.info(f"Fetched {len(df)} combined records")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise

def analyze_correlations(df: pd.DataFrame) -> None:
    """Analyze and visualize correlations between weather metrics and prices."""
    
    # Calculate correlations
    corr_columns = ['price', 'high_temp', 'low_temp', 'avg_temp', 'cdd', 'hdd']
    corr_matrix = df[corr_columns].corr()
    
    # Create correlation heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='RdBu', center=0)
    plt.title('Correlation between Weather Metrics and Natural Gas Prices')
    plt.tight_layout()
    plt.savefig('data/correlation_heatmap.png')
    plt.close()
    
    # Log correlation with price
    logger.info("\nCorrelations with Natural Gas Prices:")
    for col in corr_columns[1:]:
        corr = corr_matrix.loc['price', col]
        logger.info(f"{col}: {corr:.3f}")

def analyze_seasonal_patterns(df: pd.DataFrame) -> None:
    """Analyze seasonal patterns in prices relative to CDD and HDD."""
    
    # Add month column for seasonal analysis
    df['month'] = pd.to_datetime(df['date']).dt.month
    
    # Calculate monthly averages
    monthly_avg = df.groupby('month').agg({
        'price': 'mean',
        'cdd': 'mean',
        'hdd': 'mean'
    }).reset_index()
    
    # Create seasonal plot
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot price on primary y-axis
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Average Price', color='tab:blue')
    ax1.plot(monthly_avg['month'], monthly_avg['price'], color='tab:blue', label='Price')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    
    # Plot CDD and HDD on secondary y-axis
    ax2 = ax1.twinx()
    ax2.set_ylabel('Degree Days', color='tab:red')
    ax2.plot(monthly_avg['month'], monthly_avg['cdd'], color='tab:red', label='CDD')
    ax2.plot(monthly_avg['month'], monthly_avg['hdd'], color='tab:green', label='HDD')
    ax2.tick_params(axis='y', labelcolor='tab:red')
    
    # Add legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.title('Seasonal Patterns: Natural Gas Prices vs. Degree Days')
    plt.tight_layout()
    plt.savefig('data/seasonal_patterns.png')
    plt.close()

def analyze_extreme_weather_impact(df: pd.DataFrame) -> None:
    """Analyze price behavior during extreme weather events."""
    
    # Define extreme weather thresholds
    high_cdd_threshold = df['cdd'].quantile(0.9)  # Top 10% of CDD days
    high_hdd_threshold = df['hdd'].quantile(0.9)  # Top 10% of HDD days
    
    # Calculate average prices during extreme weather
    normal_price = df['price'].mean()
    high_cdd_price = df[df['cdd'] > high_cdd_threshold]['price'].mean()
    high_hdd_price = df[df['hdd'] > high_hdd_threshold]['price'].mean()
    
    logger.info("\nPrice Analysis during Extreme Weather:")
    logger.info(f"Average Price: ${normal_price:.2f}")
    logger.info(f"Average Price during high CDD (>{high_cdd_threshold:.1f}): ${high_cdd_price:.2f}")
    logger.info(f"Average Price during high HDD (>{high_hdd_threshold:.1f}): ${high_hdd_price:.2f}")
    
    # Calculate price volatility
    df['price_change'] = df['price'].pct_change()
    normal_vol = df['price_change'].std()
    high_cdd_vol = df[df['cdd'] > high_cdd_threshold]['price_change'].std()
    high_hdd_vol = df[df['hdd'] > high_hdd_threshold]['price_change'].std()
    
    logger.info("\nPrice Volatility Analysis:")
    logger.info(f"Normal Volatility: {normal_vol:.3f}")
    logger.info(f"Volatility during high CDD: {high_cdd_vol:.3f}")
    logger.info(f"Volatility during high HDD: {high_hdd_vol:.3f}")

def main():
    """Main function to run weather-price analysis."""
    try:
        # Fetch combined data
        df = fetch_combined_data()
        
        # Run analyses
        analyze_correlations(df)
        analyze_seasonal_patterns(df)
        analyze_extreme_weather_impact(df)
        
        logger.info("Analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 