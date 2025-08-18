import yaml
import os

config_path = './config.yaml'

if 'OPENSTACK_SUPR_SYNC_CONFIG_PATH' in os.environ:
    config_path = os.environ['OPENSTACK_SUPR_SYNC_CONFIG_PATH']
with open(config_path) as file:
    config = yaml.safe_load(file)

secrets_path = './secrets.yaml'
if 'OPENSTACK_SUPR_SYNC_SECRETS_PATH' in os.environ:
    secrets_path = os.environ['OPENSTACK_SUPR_SYNC_SECRETS_PATH']

with open(secrets_path) as file:
    secrets = yaml.safe_load(file)
