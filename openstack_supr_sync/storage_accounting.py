#!/usr/bin/env python3
import re
import logging

from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.config import config
from openstack_supr_sync.database import create_block_storage_record
from datetime import datetime
from zoneinfo import ZoneInfo

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
openstack_objects = OpenstackObjects(config['cloud_name'])

project_pattern = config['accounting']['project_pattern']

projects = openstack_objects.get_projects()
projects = [entry for entry in projects if re.search(project_pattern, entry.name)]
users = {u.id: u.name for u in openstack_objects.get_users()}
projects_lookup = {project.id: project.name for project in projects}
now = datetime.now(tz=tz).replace(tzinfo=None)
servers = [dict(project=projects_lookup[s.project_id],
                size=s.flavor.disk * (1 - any([f.delete_on_termination for f in s.volumes])) + s.flavor.ephemeral)
           for s in openstack_objects.get_servers() if s.project_id in projects_lookup]
volumes = [dict(project=projects_lookup[s.project_id], size=s.size)
           for s in openstack_objects.get_volumes() if s.project_id in projects_lookup]
backup = [dict(project=projects_lookup[s.project_id], size=s.size)
          for s in openstack_objects.get_backups() if s.project_id in projects_lookup]
project_accounting_table = {project.name: 0 for project in projects}

for p in projects:
    instance_size = sum([s['size'] for s in servers if p.name == s['project']])
    backup_size = sum([s['size'] for s in volumes if p.name == s['project']])
    volume_size = sum([s['size'] for s in backup if p.name == s['project']])
    logger.info(
        (f'Recording storage amount for project {p}: Instance {instance_size} GB,'
         f'volume {volume_size} GB, backup {backup_size} GB.'))
    create_block_storage_record(p.name, instance_size, backup_size, volume_size, now)
