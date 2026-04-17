import logging
import os
import yaml

def setup_logging(log_file_name=None):
    with open('config/config.yaml', 'r') as cfg_file:
        config = yaml.safe_load(cfg_file)

    log_dir = config['log_directory']
    os.makedirs(log_dir, exist_ok=True)

    selected_log_file = (
        log_file_name
        or os.environ.get('REPLICATOR_LOG_FILE')
        or 'replicator.log'
    )
    if os.path.isabs(selected_log_file):
        log_path = selected_log_file
    else:
        log_path = os.path.join(log_dir, selected_log_file)

    root_logger = logging.getLogger()
    configured_log_path = getattr(setup_logging, '_configured_log_path', None)
    if configured_log_path != log_path:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        setup_logging._configured_log_path = log_path

    return logging.getLogger(__name__)