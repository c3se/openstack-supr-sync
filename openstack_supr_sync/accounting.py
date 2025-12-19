#!/usr/bin/env python3
import re
import logging

from time import time, sleep
from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.connection_manager import ConnectionManager
from openstack_supr_sync.config import config, flavor_table
from openstack_supr_sync.database import update_usage, migrate_usage_entries_to_record
from datetime import datetime
from zoneinfo import ZoneInfo
from openstack_supr_sync.signal_handler import SignalHandler

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)

project_pattern = config['accounting']['project_pattern']

signal_handler = SignalHandler()

while not signal_handler.shutdown_requested:
    timeout = 0
    start_dt = datetime.now(tz=tz).replace(tzinfo=None)
    while (timeout < 3600 * 4) and not signal_handler.shutdown_requested:
        start = time()
        projects = openstack_objects.get_projects()
        projects = [entry for entry in projects if re.search(project_pattern, entry.name)]
        projects_lookup = {project.id: project.name for project in projects}
        now = datetime.now(tz=tz).replace(tzinfo=None)
        servers = [(s.flavor.name, s.project_id)
                   for s in openstack_objects.get_servers() if s.project_id in projects_lookup]
        project_accounting_table = {project.name: 0 for project in projects}
        for flavor, sid in servers:
            logger.info(f'Server {flavor}: {projects_lookup[sid]}, cost {flavor_table[flavor]}')
            project_accounting_table[projects_lookup[sid]] += flavor_table[flavor]
        for p in project_accounting_table:
            logger.info(f'Update usage for project {p}: {project_accounting_table[p]} coins')
            update_usage(p, project_accounting_table[p], now)
        sleep_time = 0
        local_timeout = max(0, 30 + start - time())
        while not signal_handler.shutdown_requested:
            # Interruptible rest
            if sleep_time >= local_timeout:
                break
            sleep(max(0, min(0.5, local_timeout - sleep_time)))
            sleep_time += 0.5
        timeout += time() - start
    logger.info("Migrating entries to record")
    migrate_usage_entries_to_record(since_time=start_dt)
