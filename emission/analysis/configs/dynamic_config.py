# moved to emission/core/deployment_config.py
# keep this file for compatibility with old code until we are sure we have
# updated all references (including in admin dashboard and public dashboard)

import emission.core.deployment_config as ecdc

def get_dynamic_config():
    return ecdc.get_deployment_config()
