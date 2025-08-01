import yaml
import os

config_path = './config.yaml'

if 'OPENSTACK_SUPR_SYNC_CONFIG_PATH' in os.environ:
    config_path = os.environ['OPENSTACK_SUPR_SYNC_CONFIG_PATH']
with open(config_path) as file:
    config = yaml.safe_load(file)
