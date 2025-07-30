import datetime
import itertools
from .supr import SUPR, SUPRHTTPError
from config import config
from connection_manager import ConnectionManager
from openstack_objects import OpenstackObjects

connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)


def adjust_allocation(project, resource_suprid, remove_allocations, add_allocations, dry_run):
    """
    Adjusts the allocation from now until the end of the project.
    """
    resource = Resource.objects.get(suprid=resource_suprid)

    try:
        queue = Queue.objects.get(resource=resource)
    except Queue.MultipleObjectsReturned:
        # Must first find special queues.
        if has_private_queue(project.name):
            # Try to determine this from existing grants, else, give up.
            # HARDCODED ASSUMPTION: Last allocation decides queue names, AND we will never change history.
            # This assumes that only the very last allocation will *ever* differ.
            last_alloc = sorted(project.allocation_set.all(),
                                key=lambda x: x.end_date)[-1]
            queue_name = last_alloc.queue.name
        else:
            queue_name = resource.name.lower()

        queue = Queue.objects.get(resource=resource, name=queue_name)
    if dry_run:
        for allocation in add_allocations:
            print('Would + {0} - {1} : {2} on queue {3} for {4}'.format(
                allocation['start_date'], allocation['end_date'],
                allocation['allocated'], queue_name, project.name))
        for allocation in remove_allocations:
            print('Would - {0} - {1} : {2} on queue {3} for {4}.'.format(
                allocation['start_date'], allocation['end_date'],
                allocation['allocated'], queue_name, project.name))
    else:
        Allocation.remove_allocations(project, queue, remove_allocations)
        Allocation.add_allocations(project, queue, add_allocations)


def import_project_data(supr_proj, tengil_proj, dry_run=False):
    supr_end_date = datetime.datetime.strptime(
        supr_proj.end_date, "%Y-%m-%d").date()
    if supr_end_date != tengil_proj.end_date:
        print("{0}: End date {1} -> {2}".format(tengil_proj.name,
              tengil_proj.end_date, supr_end_date))
        if not dry_run:
            tengil_proj.end_date = supr_end_date
            tengil_proj.save()

    supr_start_date = datetime.datetime.strptime(
        supr_proj.start_date, "%Y-%m-%d").date()
    if supr_start_date != tengil_proj.start_date:
        print("{0}: Start date {1} -> {2}".format(tengil_proj.name,
              tengil_proj.start_date, supr_start_date))
        if not dry_run:
            tengil_proj.start_date = supr_start_date
            tengil_proj.save()
    # faster than repeated DB calls inside the loop:
    tengil_db_allocations = tengil_proj.allocation_set.all(
    ).prefetch_related('queue__resource')

    for supr_rp in supr_proj['resourceprojects']:
        # We get some non-c3se resources when projects have multiple allocations. We must skip these:
        if supr_rp['resource']['centre']['name'] != 'C3SE':
            continue

        resource_suprid = supr_rp['resource']['id']
        # Allocations in SUPR
        supr_allocations = supr_rp['allocations']
        for a in supr_rp['allocations']:
            a['start_date'] = datetime.datetime.strptime(
                a['start_date'], "%Y-%m-%d").date()
            a['end_date'] = datetime.datetime.strptime(
                a['end_date'], "%Y-%m-%d").date()
            del a['id']

        tengil_allocations = list()
        for allocation in tengil_db_allocations:
            a = {
                'start_date': allocation.start_date,
                'end_date': allocation.end_date,
                'allocated': allocation.allocation,
            }
            if allocation.allocation2 is not None:
                a['allocated_2'] = allocation.allocation2
            tengil_allocations.append(a)

        if tengil_allocations != supr_allocations:
            print("{0}: Allocation differs, fixing".format(tengil_proj.name))
            remove_allocations = [
                k for k in tengil_allocations if k not in supr_allocations]
            add_allocations = [
                k for k in supr_allocations if k not in tengil_allocations]
            adjust_allocation(tengil_proj, resource_suprid,
                              remove_allocations, add_allocations, dry_run)


def import_project_members(supr_proj, openstack_project, dry_run=False):
    supr_users = supr_proj.members
    supr_accounts = []
    for user in supr_users:
        for account in user.accounts:
            if account.resource.id == config['resource_id']:
                supr_accounts.append(account.resource.username)
    supr_account_set = set(supr_accounts)
    openstack_account_set = {u.id for u in openstack_project.members}

    users_to_remove = openstack_account_set - supr_account_set
    users_to_add = supr_account_set - openstack_account_set

    for user_id in users_to_remove:
        if not dry_run:
            openstack_objects.remove_user_from_project(user_id, openstack_project.id)

            # Email User
            # tengil_person.send_email("Removed from project {0} at C3SE, Chalmers".format(tengil_proj.name),
            #                          "view/email_user_removed_from_project.txt",
            #                          extra_dict=dict(user=tengil_person,
            #                                          project=tengil_proj,
            #                                          resources=', '.join(list_of_resources)))

        print(f'Removing user {user_id} from project {openstack_project.name}')

    # Adding members.
    for user_id in users_to_add:
        if not dry_run:
            openstack_objects.add_user_to_project(user_id, openstack_project.id)
            # tengil_person.send_email("Added to project {0} at C3SE, Chalmers".format(tengil_proj.name),
            #                          "view/email_user_added_to_project.txt",
            #                          extra_dict=dict(user=tengil_person,
            #                                          project=tengil_proj,
            #                                          has_compute=has_compute,
            #                                          has_storage=has_storage,
            #                                          missing_account=missing_account,
            #                                          resources=', '.join(list_of_resources)))

        print(f'Adding user {user_id} to project {openstack_project.name}')


def disable_expired_projects(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {'resource_id': config['resource_id']}  # C3SE_CLOUD
    try:
        supr_projects = supr.get('/project/search/', params=params)
    except SUPRHTTPError as e:
        print("HTTP error {0} from SUPR:".format(e.status_code))
        raise e
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    for supr_project in supr_projects.matches:
        if datetime.date.fromisoformat(supr_project.expires) < datetime.date.today():
            openstack_objects.update_project(openstack_projects[supr_project.name], state='disabled')


def import_supr_projects(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {
        'resource_id': config['resource_id'],  # C3SE
        'end_date_ge': datetime.date.today() - datetime.timedelta(days=30),
    }
    try:
        supr_projects = supr.get('/project/search/', params=params)
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        print("HTTP error {0} from SUPR:".format(e.status_code))
        print(e.text)
        raise
    if verbose:
        print("Currently there are {0} active projects at C3SE present in SUPR".format(
            len(supr_projects.matches)))
    # active_project_requests = ProjectRequest.objects.all().values_list('suprid', flat=True)
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    for supr_project in supr_projects.matches:
        if supr_project.directory_name in openstack_projects:
            openstack_project = openstack_projects[supr_project.name]
        else:
            openstack_project = openstack_objects.create_project(supr_project.name)
        import_project_data(supr_project, openstack_project, dry_run)
        import_project_members(supr_project, openstack_project, dry_run)
        # hopefully we will never need this...
        # ldap_update_tengil_project(ldap_connect(), tengil_proj, dry_run)


def update_account_in_supr(dry_run=False, verbose=False):
    def account_open_or_closed(account):
        if account.enabled:
            if account.is_active():
                return "enabled", "Account is open"
            else:
                return "disabled", "Account is closed"
        else:
            return "disabled", "Account is disabled"
    supr = SUPR()
    openstack_accounts = openstack_objects.get_users()
    # Get Resource including account information from SUPR
    supr_resource = supr.get('/resource/{0}/'.format(config['resource_id']))
    supr_set = {(a.username, a.status) for a in supr_resource.accounts}
    tengil_set = {(o.id, o.status) for o in openstack_accounts}
    update_accounts_in_supr = tengil_set - supr_set
    for openstack_id, status in update_accounts_in_supr:
        params = {"status": status}
        try:
            if verbose:
                print(
                    "Updated account {0} to status \"{1}\"".format(openstack_id, status))
            if not dry_run:
                supr.post(
                    '/resource/{0}/account/{1}/update/'.format(config['resource_id'], openstack_id), params)
        except SUPRHTTPError as e:
            print("{0}: HTTP error {1} from SUPR: {2}".format(
                openstack_id, e.status_code, e.text))


def import_users_from_account_requests(dry_run=False, verbose=False):
    supr = SUPR()
    supr_resource = supr.get('/resource/%d/' % config['resource_suprid'])
    openstack_users = openstack_objects.get_users()
    openstack_user_names = [o.name for o in openstack_users]
    for ar in supr_resource.accountrequests:
        if ar.status != 'Active':
            continue
        for username in ar.requested_usernames:
            if username not in openstack_user_names:
                break
        else:
            if len(ar.requested_usernames) > 0:
                base_username = ar.requested_usernames[0]
            else:
                base_username = ar.person.first_name[:8]
                if base_username not in openstack_user_names:
                    username = base_username
                    break
            for i in itertools.count(0, 1):
                username = base_username + str(i)
                if username not in openstack_user_names:
                    break
        try:
            if verbose:
                print(
                    "Created account {0}".format(username))
            if not dry_run:
                openstack_user = openstack_objects.create_user(username,
                                                               status='disabled')
                params = {
                    "username": openstack_user.id,
                    "person_id": ar.person.id,
                    "resource_id": config['resource_suprid'],
                    "status": 'disabled',
                    "note": f'username: {username}'}
                supr.post('/account/create/', params)
        except SUPRHTTPError:
            openstack_objects.delete_user(openstack_user.id)
            print("Cannot connect to SUPR, deleting user!")
