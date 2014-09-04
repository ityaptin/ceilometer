from ceilometer import service
from oslo.config import cfg
from ceilometer import storage

def clear_and_dbsync(config_file):
    service.prepare_service(argv=['/', '--config-file', config_file])
    storage_conn = storage.get_connection_from_config(cfg.CONF)
    try:
        storage_conn.clear()
    except Exception as e:
        print("Error at connection clear")
    try:
        storage_conn.upgrade()
    except Exception as e:
        print("Error at connection upgrade")