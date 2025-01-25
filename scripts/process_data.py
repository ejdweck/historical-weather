import json
import zstandard as zstd
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
from pathlib import Path
import logging
import sys
from dotenv import load_dotenv

# Add the parent directory to sys.path to allow imports from app
sys.path.append(str(Path(__file__).parent.parent))

from app.db.models import FuturesData
from app.db.database import engine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_zst_file(file_path: Path) -> list:
    """Read and decompress a zst file containing line-delimited JSON data."""
    try:
        data = []
        logger.info("Opening zst file...")
        
        with open(file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(compressed_file) as reader:
                # Read and process the file in chunks
                buffer = ""
                while True:
                    chunk = reader.read(8192).decode('utf-8')  # 8KB chunks
                    if not chunk:
                        break
                    
                    buffer += chunk
                    lines = buffer.split('\n')
                    
                    # Process all complete lines
                    for line in lines[:-1]:
                        if line.strip():
                            try:
                                json_obj = json.loads(line)
                                data.append(json_obj)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Skipping invalid JSON line: {e}")
                    
                    # Keep the last potentially incomplete line in buffer
                    buffer = lines[-1]
                
                # Process the last line if it exists
                if buffer.strip():
                    try:
                        json_obj = json.loads(buffer)
                        data.append(json_obj)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
        
        total_records = len(data)
        logger.info(f"Successfully read {total_records:,} records from file")
        
        if not data:
            raise ValueError("No valid data was read from the file")
            
        return data
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except zstd.ZstdError as e:
        logger.error(f"Zstandard decompression error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}", exc_info=True)
        raise

def process_futures_data(data: list) -> pd.DataFrame:
    """Process futures data into a pandas DataFrame."""
    try:
        # Flatten the nested JSON structure
        flattened_data = []
        for record in data:
            flat_record = {
                'timestamp': record['hd']['ts_event'],
                'instrument_id': record['hd']['instrument_id'],
                'symbol': record['symbol'],
                'open': float(record['open']),
                'high': float(record['high']),
                'low': float(record['low']),
                'close': float(record['close']),
                'volume': int(record['volume'])
            }
            flattened_data.append(flat_record)
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)
        
        logger.info("Actual columns in data:")
        logger.info(df.columns.tolist())
        logger.info("\nFirst few rows of processed data:")
        logger.info(df.head())
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info(f"Processed {len(df)} rows of data")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        logger.error("Data sample for debugging:")
        if 'df' in locals():
            logger.error(df.head().to_string())
        raise

def insert_futures_data(df: pd.DataFrame, batch_size: int = 1000) -> None:
    """Insert futures data into database in batches."""
    try:
        logger.info("Verifying database connection...")
        with engine.connect() as conn:
            logger.info("Database connection successful")
        
        total_rows = len(df)
        total_inserted = 0
        
        with Session(engine) as session:
            for start_idx in range(0, total_rows, batch_size):
                try:
                    end_idx = min(start_idx + batch_size, total_rows)
                    batch_df = df.iloc[start_idx:end_idx]
                    
                    # Create FuturesData objects
                    futures_records = [
                        FuturesData(
                            timestamp=row.timestamp,
                            instrument_id=row.instrument_id,
                            symbol=row.symbol,
                            open=row.open,
                            high=row.high,
                            low=row.low,
                            close=row.close,
                            volume=row.volume
                        )
                        for row in batch_df.itertuples()
                    ]
                    
                    session.add_all(futures_records)
                    session.commit()
                    
                    total_inserted += len(batch_df)
                    logger.info(f"Progress: {total_inserted}/{total_rows} records inserted")
                    
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error inserting batch {start_idx}-{end_idx}: {str(e)}")
                    logger.error("Problematic data sample:")
                    logger.error(batch_df.head())
                    raise
                
        # Verify final count
        with Session(engine) as session:
            final_count = session.query(FuturesData).count()
            logger.info(f"Total records in database: {final_count}")
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise

def main():
    """Main function to process and insert futures data."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Set up file path
        data_file = Path(__file__).parent.parent / "data" / "glbx-mdp3-20200125-20250124.ohlcv-1d.json.zst"
        
        logger.info(f"Processing file: {data_file}")
        
        # Read and process data
        raw_data = read_zst_file(data_file)
        df = process_futures_data(raw_data)
        
        # Insert data
        logger.info(f"Inserting {len(df)} records into database...")
        insert_futures_data(df)
        
        logger.info("Processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 