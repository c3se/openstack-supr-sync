#!/usr/bin/env python3
import logging
import xml.etree.ElementTree as ET

from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.config import config
from openstack_supr_sync.database import (migrate_usage_entries_to_record, get_entry_records, archive_entry, clean_old_entries)
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo('Europe/Stockholm')
logger = logging.getLogger(__name__)
openstack_objects = OpenstackObjects(config['cloud_name'])
record_info = config['record_info']
center = record_info['center']
resource = record_info['resource']
project_pattern = config['accounting']['project_pattern']
spool_dir = config['accounting']['spool_directory']

since_time = datetime.now(tz=tz).replace(tzinfo=None) - timedelta(seconds=1)
migrate_usage_entries_to_record(since_time=since_time)
clean_old_entries()

records = get_entry_records()
xmlstrings = {}
root = ET.Element('cr:CloudRecords')
root.set('xmlns:cr', 'http://sams.snic.se/namespaces/2016/04/cloudrecords')


def append_element(stem, label, value):
    elem = ET.Element(label)
    elem.text = value
    stem.append(elem)


for r in records:
    cr = ET.SubElement(root, 'cr:CloudComputeRecord')
    now = datetime.now()
    create_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    record_id = f'{center}/{resource}/cr/{r["instance_id"]}/{now.timestamp():.0f}'
    record_id_element = ET.Element('cr:RecordIdentity')
    record_id_element.set('cr:createTime', create_time)
    record_id_element.set('cr:recordId', record_id)
    cr.append(record_id_element)
    pairs = {'Resource': resource,
             'Site': center,
             'Project': r['project_id'],
             'User': r['user'],
             'InstanceId': r['instance_id'],
             'StartTime': r['start_time'].strftime('%Y-%m-%dT%H:%M:%SZ'),
             'EndTime': r['stop_time'].strftime('%Y-%m-%dT%H:%M:%SZ'),
             'Duration': f'PT{(r["stop_time"] - r["start_time"]).total_seconds():f}S',
             'Region': 'N/A',
             'Zone': r['zone'],
             'Flavour': r['flavor'],
             'Cost': str(r['usage']),
             'AllocatedCPU': str(r['allocated_cpu']),
             'AllocatedMemory': str(r['allocated_memory'] * 1024 ** 2),
             'AllocatedDisk': str(r['allocated_disk'] * 1024 ** 3)}
    for label, value in pairs.items():
        append_element(cr, 'cr:' + label, value)
    xmlstrings[r['instance_id']] = ET.tostring(cr)
tree = ET.ElementTree(root)
tree.write(f'{spool_dir}/cloud_compute_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.xml')
for r in records:
    archive_entry(instance_id=r['instance_id'],
                  lower_timestamp=r['start_time'],
                  upper_timestamp=r['stop_time'],
                  xmlstring=xmlstrings[r['instance_id']])
