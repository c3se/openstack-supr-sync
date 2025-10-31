#!/usr/bin/env python3
import logging
import xml.etree.ElementTree as ET

from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.connection_manager import ConnectionManager
from openstack_supr_sync.config import config
from openstack_supr_sync.database import (get_block_storage_records, archive_block_storage_record)
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)
record_info = config['record_info']
storage_media = record_info['storage_media']
share = record_info['storage_share']
center = record_info['center']
resource = record_info['storage_resource']
project_pattern = config['accounting']['project_pattern']

since_time = datetime.now(tz=tz).replace(tzinfo=None) - timedelta(seconds=1)

records = get_block_storage_records()
root = ET.Element('sr:StorageUsageRecords')
root.set('xmlns:sr', 'http://eu-emi.eu/namespaces/2011/02/storagerecord')


def append_element(stem, label, value):
    elem = ET.Element(label)
    elem.text = value
    stem.append(elem)


for r in records:
    sr = ET.SubElement(root, 'sr:StorageUsageRecord')
    now = datetime.now()
    create_time = now.strftime('%Y-%m-%dT%H:%M:%S')
    record_id = f'{center}_{resource}_{r["project_id"]}_{now.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    record_id_element = ET.Element('sr:RecordIdentity')
    record_id_element.set('sr:createTime', create_time)
    record_id_element.set('sr:recordId', record_id)
    sr.append(record_id_element)
    subject_id = ET.SubElement(sr, 'sr:SubjectIdentity')
    local_group = ET.SubElement(subject_id, 'sr:LocalGroup')
    local_group.text = r['project_id']
    total_storage = r['instance_usage'] + r['volume_usage'] + r['backup_usage']
    total_storage *= 1024 ** 3
    pairs = {'StorageSystem': resource,
             'StorageShare': share,
             'Site': center,
             'StorageMedia': storage_media,
             'StartTime': r['timestamp'].strftime('%Y-%m-%dT%H:%M:%SZ'),
             'EndTime': r['timestamp'].strftime('%Y-%m-%dT%H:%M:%SZ'),
             'ResourceCapacityUsed': str(int(total_storage)),
             'LogicalCapacityUsed': str(int(total_storage)),
             }
    for label, value in pairs.items():
        append_element(sr, 'sr:' + label, value)
    archive_block_storage_record(r['project_id'], r['timestamp'])
tree = ET.ElementTree(root)
tree.write(f'cloud_storage_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.xml')
