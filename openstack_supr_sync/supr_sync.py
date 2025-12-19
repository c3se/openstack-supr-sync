import datetime
import itertools
import logging
import re
from openstack_supr_sync.supr import SUPR, SUPRHTTPError
from openstack_supr_sync.utils import get_profanity_score
from openstack_supr_sync.config import config
from openstack_supr_sync.connection_manager import ConnectionManager
from openstack_supr_sync.openstack_objects import OpenstackObjects
from openstack_supr_sync.database import get_usage_since_time

logger = logging.getLogger(__name__)

connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)


def import_project_members(supr_proj, openstack_project, supr_resource, dry_run=False):
    supr_users = supr_proj.members
    all_accounts = {a.person.id: a.username for a in supr_resource.accounts}
    supr_accounts = []
    for user in supr_users:
        if user.id in all_accounts:
            supr_accounts.append(all_accounts[user.id])
    supr_account_set = set(supr_accounts)
    openstack_id_dict = {u.name: u.id for u in openstack_objects.get_users()}
    openstack_account_set = {u for u in openstack_objects.get_project_members(openstack_project)}
    users_to_remove = openstack_account_set - supr_account_set
    users_to_add = supr_account_set - openstack_account_set

    for user in users_to_remove:
        user_id = openstack_id_dict[user]
        if not dry_run:
            openstack_objects.remove_user_from_project(openstack_project.id, user_id)

        logger.info(f'Removing user {user} [{user_id}] from project'
                    f' {openstack_project.name} [{openstack_project.id}]')

    # Adding members.
    for user in users_to_add:
        user_id = openstack_id_dict[user]
        if not dry_run:
            openstack_objects.add_user_to_project(openstack_project.id, user_id)

        logger.info(
            f'Adding user {user}, {user_id} to project {openstack_project.name}, {openstack_project.id}')


def disable_expired_projects(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {'resource_id': config['supr']['resource_id'],
              'end_date_le': datetime.date.today()}

    try:
        supr_projects = supr.get('/project/search/', params=params)
    except SUPRHTTPError as e:
        logger.info("HTTP error {0} from SUPR:".format(e.status_code))
        raise e
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    for supr_project in supr_projects.matches:
        if verbose:
            logger.info(f'Disabling {supr_project.name}')
        if not dry_run:
            openstack_objects.update_project(
                openstack_projects[supr_project.name], is_enabled=False)


def update_project_openstack_quotas(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {
        'resource_id': config['supr']['resource_id'],  # C3SE
        'end_date_ge': datetime.date.today() - datetime.timedelta(days=30)}
    supr_projects = supr.get('/project/search/', params=params)
    supr_project_names = [p.name for p in supr_projects.matches]
    supr_project_allocations = {p.name: p.resourceprojects for p in supr_projects.matches}
    for name in supr_project_names:
        for r in supr_project_allocations[name]:
            if int(r.resource.id) == int(config['supr']['resource_id']):
                resource = r
            elif int(r.resource.id) == int(config['supr']['storage_id']):
                storage = r
        # TiB to GiB
        supr_project_allocations[name] = dict(coins=resource.allocated,
                                              storage=storage.allocated * 1000)
    openstack_projects = {
        o.name: o.id for o in openstack_objects.get_projects()
        if o.name in supr_project_names}
    for p, p_id in openstack_projects.items():
        openstack_objects.set_project_storage_quota(
            p_id,
            storage_in_gb=supr_project_allocations[p]['storage'],
            number_of_snapshots=100,
            number_of_volumes=100,
            number_of_backups=100)
    current_time = datetime.datetime.now()
    past_time = current_time - datetime.timedelta(days=30)
    limited_quota = dict(cores=1, instances=1, ram=2048)
    default_quota = dict(cores=256, instances=256, ram=2048*256*4)
    for p, p_id in openstack_projects.items():
        usage = get_usage_since_time(p_id, past_time)
        if usage is None:
            if not dry_run:
                openstack_objects.set_project_quota(p, default_quota)
            continue
        if verbose:
            logger.info(f'Project: {p} [{p_id}]')
            logger.info(f'Allocation: {supr_project_allocations[p]["coins"]} coins per 30 days')
            logger.info(f'Usage: {usage} for the past 30 days')

        if usage > float(supr_project_allocations[p]['coins']):
            quota = limited_quota
            if verbose:
                logger.info(f'Limiting quota for project {p}')
        else:
            quota = default_quota
        if not dry_run:
            openstack_objects.set_project_quota(p, quota)


def disable_and_enable_openstack_accounts(dry_run=False, verbose=False):
    supr = SUPR()
    params = {
        'resource_id': config['supr']['resource_id'],  # C3SE
        'end_date_ge': datetime.date.today() - datetime.timedelta(days=30),
    }
    try:
        supr_projects = supr.get('/project/search/', params=params)
        supr_resource = supr.get(f'/resource/{config["supr"]["resource_id"]}')
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        logger.info("HTTP error {0} from SUPR:".format(e.status_code))
        logger.info(e.text)
        raise

    if verbose:
        logger.info("Currently there are {0} active projects at C3SE present in SUPR".format(
            len(supr_projects.matches)))
    # active_project_requests = ProjectRequest.objects.all().values_list('suprid', flat=True)
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    active_members = set()
    supr_accounts = {a.username for a in supr_resource.accounts}
    for supr_project in supr_projects.matches:
        if openstack_projects[supr_project.name].is_enabled:
            active_members |= set(openstack_objects.get_project_members(
                openstack_projects[supr_project.name]))
    accounts_without_projects = supr_accounts - active_members
    for user in accounts_without_projects:
        openstack_objects.update_user(user, is_enabled=False)
    for user in active_members:
        openstack_objects.update_user(user, is_enabled=True)


def import_supr_projects(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {
        'resource_id': config['supr']['resource_id'],  # C3SE
        'end_date_ge': datetime.date.today() - datetime.timedelta(days=30),
    }
    try:
        supr_projects = supr.get('/project/search/', params=params)
        supr_resource = supr.get(f'/resource/{config["supr"]["resource_id"]}')
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        logger.info("HTTP error {0} from SUPR:".format(e.status_code))
        logger.info(e.text)
        raise

    if verbose:
        logger.info("Currently there are {0} active projects at C3SE present in SUPR".format(
            len(supr_projects.matches)))
    # active_project_requests = ProjectRequest.objects.all().values_list('suprid', flat=True)
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    for supr_project in supr_projects.matches:
        if supr_project.name in openstack_projects:
            openstack_project = openstack_projects[supr_project.name]
        else:
            openstack_project = openstack_objects.create_project(supr_project.name)
        import_project_members(supr_project, openstack_project, supr_resource, dry_run)


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
    supr_resource = supr.get(f'/resource/{config["supr"]["resource_id"]}')
    supr_set = {(a.username, a.status) for a in supr_resource.accounts}
    supr_names = [a.username for a in supr_resource.accounts]
    status_map = {True: 'enabled', False: 'disabled'}
    tengil_set = {(o.name, status_map[o.is_enabled]) for o in openstack_accounts if o.name in supr_names}
    update_accounts_in_supr = tengil_set - supr_set
    for openstack_id, status in update_accounts_in_supr:
        params = {"status": status}
        try:
            if verbose:
                logger.info(
                    f'Updated account {openstack_id} to status "{status}"')
            if not dry_run:
                supr.post(
                    f'/resource/{config["supr"]["resource_id"]}/account/{openstack_id}/update/',
                    params)
        except SUPRHTTPError as e:
            logger.info(f'{openstack_id}: HTTP error {e.status_code} from SUPR: {e.text}')


def import_users_from_account_requests(dry_run=False, verbose=False):
    supr = SUPR()
    supr_resource = supr.get(f'/resource/{config["supr"]["resource_id"]}')
    openstack_users = openstack_objects.get_users()
    openstack_user_names = [o.name for o in openstack_users]
    for ar in supr_resource.accountrequests:
        username = None
        if ar.status.lower() != 'active':
            continue
        for un in ar.requested_usernames:
            un = un.lower()
            if not re.match(r'^[a-z0-9_-]+$', un):
                continue
            if un not in openstack_user_names:
                if get_profanity_score(un) < 0.95:
                    username = un
                    break
        if username is None:
            base_username = ar.person.first_name[:8].lower()
            if base_username not in openstack_user_names:
                username = base_username
            else:
                for i in itertools.count(0, 1):
                    username = base_username + str(i)
                    if username not in openstack_user_names:
                        break
        try:
            if verbose:
                logger.info(
                    "Created account {0}".format(username))
            if not dry_run:
                openstack_user = openstack_objects.create_user(username,
                                                               status='disabled')
                params = {
                    "username": username,
                    "person_id": ar.person.id,
                    "resource_id": config['supr']['resource_id'],
                    "status": 'disabled'}
                supr.post('/account/create/', params)
                openstack_user_names.append(username)
        except SUPRHTTPError:
            openstack_objects.delete_user(openstack_user.id)
            logger.info("Cannot connect to SUPR, deleting user!")


if __name__ == '__main__':
    import_users_from_account_requests(verbose=True)
    import_supr_projects(verbose=True)
    disable_and_enable_openstack_accounts(verbose=True)
    disable_expired_projects(verbose=True)
    update_account_in_supr(verbose=True)
