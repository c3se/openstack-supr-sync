#!/bin/env python

import sys
import optparse
from os import environ
from os.path import dirname as dn, realpath as rp

sys.path.append(dn(dn(rp(__file__))))
environ['DJANGO_SETTINGS_MODULE'] = 'settings'
import django; django.setup()


from bureaucracy.supr_sync import *


parser = optparse.OptionParser(usage="""%prog [options]

Script to add CENTRE ID to Person in SUPR. User in Tengil must have SUPR ID set.
Optionally, only specific parts can be imported by giving e.g. --projects (default is all)
""")

parser.add_option('', '--days', type='int', dest="days", default=30,
                  help='Accounts active until max(project_end_date) + days ' '[default: %default]')
parser.add_option('', '--verbose', action='store_true', dest="verbose", default=False,
                  help='Verbose mode.')
parser.add_option('', '--dry-run', action='store_true', dest="dry_run", default=False,
                  help='If specified lists Tengil Users with no SUPR ID set. _No_ update is done')

parser.add_option('', '--projects', action='store_true', default=False)
parser.add_option('', '--ar', action='store_true', default=False)
parser.add_option('', '--groups', action='store_true', default=False)
parser.add_option('', '--mstud', action='store_true', default=False)
parser.add_option('', '--metadata', action='store_true', default=False)
parser.add_option('', '--end_date', action='store_true', default=False)
parser.add_option('', '--centre_id', action='store_true', default=False)

(options, args) = parser.parse_args()

select = options.projects or options.ar or options.groups or options.mstud or options.metadata \
         or options.centre_id or options.end_date

# Order matters; We sync projects first, as can influence account-requests (due to MStud)
if not select or options.projects:
    # Got to update SUPR first to ensure we don't get duplicates
    import_supr_projects(dry_run=options.dry_run, verbose=options.verbose)
if not select or options.ar:
    update_account_in_supr(dry_run=options.dry_run, verbose=options.verbose)
    import_account_requests(dry_run=options.dry_run, verbose=options.verbose)

# Order matters:
if not select or options.centre_id:
    update_centre_id_in_supr(dry_run=options.dry_run, verbose=options.verbose)
if not select or options.metadata:
    import_user_metadata(dry_run=options.dry_run, verbose=options.verbose)

# Order of these do not matter:
if not select or options.groups:
    import_group_members(dry_run=options.dry_run, verbose=options.verbose)
if not select or options.end_date:
    update_account_end_date(options.days, dry_run=options.dry_run, verbose=options.verbose)
