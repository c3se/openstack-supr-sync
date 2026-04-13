#!/usr/bin/env python3
import re
import logging

from time import time, sleep
from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.config import config, flavor_table
from openstack_supr_sync.database import update_usage
from datetime import datetime
from zoneinfo import ZoneInfo
from openstack_supr_sync.signal_handler import SignalHandler

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
openstack_objects = OpenstackObjects(config['cloud_name'])
max_time = config['accounting']['sampling_frequency']

project_pattern = config['accounting']['project_pattern']

signal_handler = SignalHandler()

while not signal_handler.shutdown_requested:
    start = time()
    projects = openstack_objects.get_projects()
    projects = [entry for entry in projects if re.search(project_pattern, entry.name)]
    users = {u.id: u.name for u in openstack_objects.get_users()}
    projects_lookup = {project.id: project.name for project in projects}
    now = datetime.now(tz=tz).replace(tzinfo=None)
    servers = [dict(instance_id=s.id,
                    flavor=s.flavor.name,
                    project=projects_lookup[s.project_id],
                    user=users.get(s.user_id, ''),
                    zone=s.availability_zone,
                    allocated_cpu=s.flavor.vcpus,
                    allocated_disk=s.flavor.disk + s.flavor.ephemeral + s.flavor.swap,
                    allocated_memory=s.flavor.ram,
                    state=s.status)
               for s in openstack_objects.get_servers() if s.project_id in projects_lookup]
    project_accounting_table = {project.name: 0 for project in projects}
    for sd in servers:
        logger.info(f'Server {sd["flavor"]}: {sd["project"]}, cost {flavor_table[sd["flavor"]]}')
        # third argument is a dict that becomes a jsonb blob and is extracted at reporting time
        if sd.pop('state').lower() != 'shelved_offloaded':
            cost = flavor_table[sd["flavor"]]
        else:
            cost = 0.
        update_usage(sd.pop('project'), sd.pop('instance_id'), sd, cost, now)
    sleep_time = 0.
    local_timeout = max(0, max_time + start - time())
    while not signal_handler.shutdown_requested:
        # Interruptible rest
        if sleep_time >= local_timeout:
            break
        sleep(max(0, min(0.5, local_timeout - sleep_time)))
        sleep_time += 0.5
