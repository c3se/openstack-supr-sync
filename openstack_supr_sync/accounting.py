import re
import logging

from time import time, sleep
from .openstack_objects import OpenstackObjects
from .connection_manager import ConnectionManager
from .config import config, flavor_table
from .database import update_usage, migrate_usage_entries_to_record
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .signal_handler import SignalHandler

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)

project_pattern = 'C3SE \d{4}/\d{2,3}-\d+'

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
       servers =  [(s.flavor.name, s.project_id) for s in openstack_objects.get_servers() if s.project_id in projects_lookup]
       project_accounting_table = {project.name: 0 for project in projects}
       for flavor, sid in servers:
           logger.info(f'Server {flavor}: {projects_lookup[sid]}, cost {flavor_table[flavor]}')
           project_accounting_table[projects_lookup[sid]] += flavor_table[flavor]
       for p in project_accounting_table:
           logger.info(f'Update usage for project {p}: {project_accounting_table[p]} coins')
           update_usage(p, project_accounting_table[p], now)
       if not signal_handler.shutdown_requested:
           sleep(max(0, 30 + start - time()))
           timeout += time() - start
    logger.info("Migrating entries to record")
    migrate_usage_entries_to_record(since_time=start_dt)
