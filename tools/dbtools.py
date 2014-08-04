from ceilometer import service
from oslo.config import cfg
from ceilometer import storage

def clear_and_dbsync(config_file):
    service.prepare_service(argv=['/', '--config-file', config_file])
    storage_conn = storage.get_connection_from_config(cfg.CONF)
    storage_conn.clear()
    storage_conn.upgrade()