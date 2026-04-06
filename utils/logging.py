import logging
import yaml

def setup_logging():
    config = yaml.safe_load(open('config/config.yaml'))
    log_dir = config['log_directory']
    logging.basicConfig(
        filename=f'{log_dir}/replicator.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)