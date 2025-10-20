#!/usr/bin/env python3
import logging

from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.connection_manager import ConnectionManager
from openstack_supr_sync.config import config
from openstack_supr_sync.database import migrate_usage_entries_to_record
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)

project_pattern = config['accounting']['project_pattern']

since_time = datetime.now(tz=tz).replace(tzinfo=None) - timedelta(hours=1)
migrate_usage_entries_to_record(since_time=since_time)
