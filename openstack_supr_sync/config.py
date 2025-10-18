import logging
import yaml
import os


path = './config.yaml'
if 'OPENSTACK_SUPR_SYNC_CONFIG_PATH' in os.environ:
    path = os.environ['OPENSTACK_SUPR_SYNC_CONFIG_PATH']
with open(path) as file:
    config = yaml.safe_load(file)

logging.basicConfig(filename=config['log_file'],
                    level=config['log_level'])

path = './secrets.yaml'
if 'OPENSTACK_SUPR_SYNC_SECRETS_PATH' in os.environ:
    path = os.environ['OPENSTACK_SUPR_SYNC_SECRETS_PATH']

with open(path) as file:
    secrets = yaml.safe_load(file)

path = './flavor_table.yaml'
if 'OPENSTACK_SUPR_SYNC_FLAVOR_PATH' in os.environ:
    path = os.environ['OPENSTACK_SUPR_SYNC_FLAVOR_PATH']

with open(path) as file:
    flavor_table = yaml.safe_load(file)
