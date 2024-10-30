import os
import urllib.parse as urlp
from dotenv import load_dotenv
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
from rich.console import Console
from loguru import logger
import time

console = Console()

def config():
    """Configure script"""
    load_dotenv(f'./config/.env')

    # Configure logger
    logger.add(
        "logs/main.log", 
        rotation="25 MB", 
        retention="30 days",
        format="{time} | {level} | {name}:{function}:{line} - {message}", 
        level="INFO"
    )

    # Ensure core folders exist
    core_dirs = ['config/', 'logs/']
    for path in core_dirs:
        os.makedirs(path, exist_ok=True)

    # Return config dictionary
    return {
        "conn_str": f'mysql+pymysql://{os.environ["DB_USER"]}:{urlp.quote_plus(os.environ["DB_PASS"])}@{os.environ["DB_HOST"]}:{os.environ["DB_PORT"]}',
        "table": os.environ["TEST_TABLE"],
        "timings": {}
    }

def timeit(func):
    """Decorator for timing a function."""
    def wrapper(cfg):
        start_time = time.perf_counter()
        result = func(cfg)
        cfg['timings'][func.__name__] = time.perf_counter() - start_time
        return result
    return wrapper

@timeit
def connect(cfg):
    """Create an sqlalchemy engine and store connection in cfg."""
    cfg['conn'] = create_engine(cfg["conn_str"]).connect()
    logger.info('\N{WHITE HEAVY CHECK MARK} Connection engine created successfully!')

@timeit
def make_test_table(cfg):
    """Create a dummy dataframe and write it to the test table."""
    dummy_df = pl.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'age': [25, 30, 35, 28],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com']
    })
    try:
        dummy_df.write_database(
            table_name=cfg['table'],
            connection=cfg['conn'],
            if_table_exists='replace'
        )
        logger.info('\N{WHITE HEAVY CHECK MARK} Test table created successfully!')
    except Exception as e:
        logger.error(f'\N{OCTAGONAL SIGN} Error making table: {e}')

@timeit
def select_table(cfg):
    """Select inserted data from test table."""
    select_sql = f"SELECT * FROM {cfg['table']}"
    result_df = pl.from_pandas(pd.read_sql(sql=select_sql, con=cfg['conn']))

    data_length = result_df.shape[0]
    if data_length <= 0:
        logger.warning(f'\N{LARGE YELLOW CIRCLE} Data length - {data_length}')
    else:
        logger.info(f'\N{WHITE HEAVY CHECK MARK} Data length - {data_length}')

    cfg['result'] = result_df  # Store result in cfg

@timeit
def cleanup_table(cfg):
    """Remove the test table from the database."""
    try:
        drop_sql = f"DROP TABLE IF EXISTS {cfg['table']};"
        cfg['conn'].execute(text(drop_sql))
        logger.info(f'\N{WHITE HEAVY CHECK MARK} Table {cfg["table"]} dropped successfully!')
    except Exception as e:
        logger.error(f'\N{OCTAGONAL SIGN} Error dropping table: {e}')

if __name__ == "__main__":
    # Collect configuration.
    cfg = config()

    # Connect to database.
    connect(cfg)

    # Make test table.
    make_test_table(cfg)

    # Select data from test table.
    select_table(cfg)

    # Cleanup table and close connection.
    cleanup_table(cfg)
    cfg['conn'].close()

    # Print result & timings.
    console.print(cfg['result'])
    console.print_json(data=cfg['timings'])