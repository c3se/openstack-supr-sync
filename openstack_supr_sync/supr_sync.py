import datetime
import itertools
from .supr import SUPR, SUPRHTTPError
from .config import config
from .connection_manager import ConnectionManager
from .openstack_objects import OpenstackObjects

connection = ConnectionManager(config['cloud_name'])
openstack_objects = OpenstackObjects(connection)


def import_project_members(supr_proj, openstack_project, dry_run=False):
    supr_users = supr_proj.members
    supr_accounts = []
    print(supr_users)
    for user in supr_users:
        if 'accounts' in user:
            for account in user.accounts:
                if account.resource.id == config['supr']['resource_id']:
                    supr_accounts.append(account.resource.username)
    supr_account_set = set(supr_accounts)
    openstack_id_dict = {u.name: u.id for u in openstack_objects.get_users()}
    try:
        openstack_account_set = {u.name for u in openstack_project.members}
    except AttributeError:
        openstack_account_set = set()
    users_to_remove = openstack_account_set - supr_account_set
    users_to_add = supr_account_set - openstack_account_set

    for user in users_to_remove:
        user_id = openstack_id_dict[user]
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
    for user in users_to_add:
        user_id = openstack_id_dict[user]
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
    params = {'resource_id': config['supr']['resource_id'],
              'end_date_le': datetime.date.today()}

    try:
        supr_projects = supr.get('/project/search/', params=params)
    except SUPRHTTPError as e:
        print("HTTP error {0} from SUPR:".format(e.status_code))
        raise e
    openstack_projects = {o.name: o for o in openstack_objects.get_projects()}
    for supr_project in supr_projects.matches:
        if datetime.date.fromisoformat(supr_project.expires) < datetime.date.today():
            openstack_objects.update_project(openstack_projects[supr_project.name], state='disabled')


def limit_projects_without_resources(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {
        'resource_id': config['supr']['resource_id'],  # C3SE
        'end_date_ge': datetime.date.today() - datetime.timedelta(days=30)}
    # TODO: some kind of logic to limit projects that run out of currency here


def import_supr_projects(dry_run=False, verbose=False):
    supr = SUPR()
    # Search parameters
    params = {
        'resource_id': config['supr']['resource_id'],  # C3SE
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
        if supr_project.name in openstack_projects:
            openstack_project = openstack_projects[supr_project.name]
        else:
            openstack_project = openstack_objects.create_project(supr_project.name)
        # import_project_data(supr_project, openstack_project, dry_run)
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
    supr_resource = supr.get('/resource/{0}/'.format(config['supr']['resource_id']))
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
                    '/resource/{0}/account/{1}/update/'.format(config['supr']['resource_id'], openstack_id), params)
        except SUPRHTTPError as e:
            print("{0}: HTTP error {1} from SUPR: {2}".format(
                openstack_id, e.status_code, e.text))


def import_users_from_account_requests(dry_run=False, verbose=False):
    supr = SUPR()
    supr_resource = supr.get('/resource/%d/' % int(config['supr']['resource_id']))
    openstack_users = openstack_objects.get_users()
    openstack_user_names = [o.name for o in openstack_users]
    for ar in supr_resource.accountrequests:
        if ar.status != 'Active':
            continue
        for un in ar.requested_usernames:
            if un not in openstack_user_names:
                username = un
                break
        if username is None:
            if len(ar.requested_usernames) > 0:
                base_username = ar.requested_usernames[0]
            else:
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
                print(
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
        except SUPRHTTPError:
            openstack_objects.delete_user(openstack_user.id)
            print("Cannot connect to SUPR, deleting user!")
