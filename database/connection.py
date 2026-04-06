import yaml
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def get_engine():
    config = load_config()
    db_config = config['database']
    db_type = db_config.get('type', 'duckdb')
    
    if db_type == 'duckdb':
        db_path = os.path.expanduser(db_config['path'])
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        connection_string = f"duckdb:///{db_path}"
        # Configure DuckDB timezone to AEST (NEM timezone) using duckdb config options
        engine = create_engine(
            connection_string,
            connect_args={
                'config': {'timezone': 'Australia/Brisbane'}
            }
        )
    else:
        # Fallback to PostgreSQL for backward compatibility
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        engine = create_engine(connection_string)
    
    return engine

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()