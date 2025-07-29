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


def import_project_members(supr_proj, tengil_proj, dry_run=False):
    supr_users_set = {u.id for u in supr_proj.members}
    tengil_users_set = {u.SUPRID for u in tengil_proj.members.all()}

    users_to_remove = tengil_users_set - supr_users_set
    users_to_add = supr_users_set - tengil_users_set

    if users_to_remove or users_to_add:
        resources = tengil_proj.resources(
            max(tengil_proj.start_date, datetime.date.today()))
        list_of_resources = [r.name for r in resources]
        has_compute = any([r.is_compute() for r in resources])
        has_storage = any([r.is_storage() for r in resources])

    # Removing members
    for s_id in users_to_remove:
        tengil_person = User.objects.get(SUPRID=s_id)

        # Remove Tengil User from Project present in Tengil
        if not dry_run:
            tengil_proj.members.remove(tengil_person)
            tengil_proj.save()

            # Email User
            tengil_person.send_email("Removed from project {0} at C3SE, Chalmers".format(tengil_proj.name),
                                     "view/email_user_removed_from_project.txt",
                                     extra_dict=dict(user=tengil_person,
                                                     project=tengil_proj,
                                                     resources=', '.join(list_of_resources)))

        print("- {0:14}: {1}".format(tengil_proj.name,
              tengil_person.fullName().encode('utf-8')))

    # Adding members.
    for s_id in users_to_add:
        try:
            tengil_person = User.objects.get(SUPRID=s_id)
        except User.DoesNotExist:
            continue

        # Add User/Person to project
        if not dry_run:
            tengil_proj.members.add(tengil_person)
            tengil_proj.save()

            compute_resources = {r for r in resources if r.is_compute()}
            missing_account = len(
                compute_resources - {acc.resource for acc in tengil_person.account_set.all()}) > 0

            tengil_person.send_email("Added to project {0} at C3SE, Chalmers".format(tengil_proj.name),
                                     "view/email_user_added_to_project.txt",
                                     extra_dict=dict(user=tengil_person,
                                                     project=tengil_proj,
                                                     has_compute=has_compute,
                                                     has_storage=has_storage,
                                                     missing_account=missing_account,
                                                     resources=', '.join(list_of_resources)))

        print("+ {0:14}: {1}".format(tengil_proj.name,
              tengil_person.fullName().encode('utf-8')))


def import_object_expiry_date(supr_object, tengil_object,
                              dry_run=False, verbose=False):
    if 'expires' in supr_object:
        if str(tengil_object.expires) != str(supr_object.expires):
            if not dry_run:
                tengil_object.expires = supr_object.expires
                tengil_object.save()
            if verbose:
                print(f'Object {tengil_object} set to expire in SUPR,'
                      f' setting expiry date to {supr_object.expires}!'
                      ' (if not dry_run)')


def import_project_expiry_date(dry_run=False, verbose=False):
    supr = SUPR()

    # Search parameters
    params = {
        'resource_centre_id': 6,  # C3SE
        'modified_since': (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")}
    try:
        supr_projects = supr.get('/project/search/', params=params)
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        print("HTTP error {0} from SUPR:".format(e.status_code))
        print(e.text)
        raise
    for supr_project in supr_projects.matches:
        try:
            tengil_proj = Project.objects.get(suprid=supr_project.id)
        except Project.DoesNotExist:
            continue
        import_object_expiry_date(supr_project, tengil_proj, dry_run, verbose)


def import_supr_projects(dry_run=False, verbose=False):
    supr = SUPR()

    # Search parameters
    params = {
        'resource_centre_id': 6,  # C3SE
        # Only active projects (plus a few extra days in case of last second changes)
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
    openstack_projects = openstack_objects.get_projects()
    openstack_project_ids = [o.id for o in openstack_projects]
    for supr_project in supr_projects.matches:
        if supr_project.directory_name in openstack_project_ids:
            openstack_project = openstack_projects[openstack_project_ids.index(supr_project.directory_name)]
        else:
            # use directory name in SUPR to match?
            openstack_project = openstack_objects.create_project(supr_project.name)
        import_project_data(supr_project, openstack_project, dry_run)
        import_project_members(supr_project, openstack_project, dry_run)
        # hopefully we will never need this...
        # ldap_update_tengil_project(ldap_connect(), tengil_proj, dry_run)


def create_user_in_tengil(suprid, dob, pnr, dry_run=False):
    # Create new User Request (no-op if already exists)

    try:
        UserRequest.objects.get(SUPRID=suprid)
        return None, None
    except UserRequest.DoesNotExist:
        supr_user = SUPR().get('/person/' + str(suprid))
        name = supr_user.first_name + ' ' + supr_user.last_name
        if dry_run:
            print("** INFO: A User or UserRequest would have been created in Tengil for {0}, SUPRID: {1}".format(
                name.encode('utf8'), suprid))
        else:
            # Check to see if there is a definite CID already:
            pdb_person = None
            if pnr:
                try:
                    pdb_person = pdbpnr(pnr)
                except ValueError:
                    print('Hard error when trying to look up pnr: ' +
                          pnr[:6] + '-XXXX')
            if not pdb_person:
                try:
                    pdb_person = pdbemail(supr_user.email)
                except ValueError:
                    print('Hard error when trying to look up email: ' +
                          supr_user.email)
            if pdb_person:
                cid = pdb_person['CID']
                # Attempt to pair up with existing (hopefully unpaired) User with same CID.
                try:
                    # Note: This should never happen anymore: all existing users must be in SUPR.
                    user = User.objects.get(CID=cid)
                    raise ValueError(
                        f"Request for user \"{cid}\" which already seem to exist in Tengil. This shouldn't happen.")
                    # if user.SUPRID is not None or user.coupled_with_supr:
                    #    raise ValueError("Request for user which already seem to exist in Tengil. This shouldn't happen.")
                    # user.SUPRID = suprid
                    # user.coupled_with_supr = False  # sync done by update_centre_id_in_supr which creates Center ID posts in SUPR
                    # user.save()
                    # return user
                except User.DoesNotExist:
                    pass

            if pdb_person:
                cid = pdb_person['CID']
            else:
                cid = ''

            # No unmatched users then just create user directly to save time.
            return (
                User.objects.create(
                    SUPRID=suprid,
                    CID=cid,
                    firstName=supr_user.first_name, lastName=supr_user.last_name,
                    email=supr_user.email,
                    phone=supr_user.tel1,
                    attended_intro_seminar=False,
                    # sync done by update_centre_id_in_supr which creates Center ID posts in SUPR
                    coupled_with_supr=False,
                    ua_accepted_in_supr=supr_user.user_agreement_accepted),
                pdb_person)


def create_ar_in_tengil(user, resource, dob, pnr, pdb_person, dry_run=False):
    if dry_run:
        print("** INFO: An AccountRequest would have been created in Tengil for User {0} on resource {1}".format(
            user.fullName().encode('utf8'), resource))
    else:
        if dob is None:
            dob = ''
        if pnr is None:
            pnr = ''
        if user.CID == '':
            print(
                f"User {user.fullName()} has no CID: {user.CID}, creating account request!")
            AccountRequest.objects.create(
                user=user, resource=resource, dob=dob, pnr=pnr)
            print("** INFO: An AccountRequest was created in Tengil for User {0} on resource {1}".format(
                user.fullName().encode('utf8'), resource))
        else:
            print(
                f"Auto-creating {resource} account for user {user.CID} if valid...")
            if int(pdb_person['unixid']) > 1000:
                Account.objects.create(
                    user=user,
                    resource=resource,
                    unixname=user.CID,
                    unixid=int(pdb_person['unixid']))
                Account.objects.get(user=user, resource=resource).log_create(
                    DjangoUser.objects.get(username='tengil-bot'))
            else:
                print(
                    f"User {user.CID} has unixid < 1000, please create manually.")
                AccountRequest.objects.create(
                    user=user, resource=resource, dob=dob, pnr=pnr)


def get_supr_group_members(tengil_group):
    # Return a set of SUPR list members
    supr = SUPR()
    suffix = '_r' if tengil_group.SUPR_regex else ''
    try:
        supr_group = supr.get(
            '/group/search/?name{0}={1}'.format(suffix, tengil_group.SUPR_match))
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        print("HTTP error {0} from SUPR:".format(e.status_code))
        print(e.text)
        raise

    if len(supr_group['matches']) < 1:
        print('Error: group not found in SUPR: "{0}"!'.format(
            tengil_group.SUPR_match))
        return None

    members = []
    for group in supr_group['matches']:
        members.extend(group['members'])
    return members


def import_group_members(dry_run=False, verbose=False):

    for tengil_group in UnixGroup.objects.exclude(SUPR_match__exact=''):
        if verbose:
            match_type = 'regex' if tengil_group.SUPR_regex else 'name'
            print('Looking at Tengil group: {0} using SUPR-{1} "{2}"'.format(tengil_group.name,
                                                                             match_type, tengil_group.SUPR_match))

        supr_members = get_supr_group_members(tengil_group)
        if supr_members is None:
            print('Skipping futher processing of {0}'.format(
                tengil_group.name))
            continue

        # Build set of Tengil accounts for SUPR members
        supr_ids = [m['id'] for m in supr_members]
        supr_members_set = set(User.objects.filter(SUPRID__in=supr_ids))
        tengil_members = set(tengil_group.members.all())

        def members_to_str(members):
            return ', '.join([m.fullName() for m in members])

        to_add = supr_members_set - tengil_members
        to_remove = tengil_members - supr_members_set

        if verbose and (to_add or to_remove):
            print(' Changes needed:')
            if to_add:
                print('  Users missing in Tengil group:', members_to_str(to_add))
            if to_remove:
                print('  Users to remove from Tengil group:',
                      members_to_str(to_remove))

        for user in to_add:
            if not dry_run:
                print('Adding user"' + str(user) +
                      '" to UnixGroup "' + str(tengil_group) + '"')
                tengil_group.members.add(user)
            elif verbose:
                print('Users "' + str(user) +
                      '" should be added to UnixGroup "' + str(tengil_group) + '"')

        for user in to_remove:
            if not dry_run:
                print('Removing user "' + str(user) +
                      '" from UnixGroup "' + str(tengil_group) + '"')
                tengil_group.members.remove(user)
            elif verbose:
                print('User "' + str(user) +
                      '" should be removed from UnixGroup "' + str(tengil_group) + '"')


def update_account_end_date(extra_days=31, dry_run=False, verbose=False):
    # We do not want to close local accounts, "c3-", or SweGrid accounts.
    unixnames_to_exclude = (Q(unixname__startswith="c3-") |
                            Q(unixname__startswith="swegrid") |
                            Q(unixname__startswith="oneadmin"))

    extra_days = datetime.timedelta(days=extra_days)
    # Grants includes X days backwards, since we let the accounts stay open to fetch files:
    allocations = Allocation.objects.exclude(end_date__lt=datetime.date.today(
    ) - extra_days).prefetch_related('queue__resource', 'project__members')

    # Using grants is inefficient, we extract the max date for the (project, resource) pair:
    end_dates_tmp = dict()
    for allocation in allocations:
        if allocation.allocation == 0:
            continue
        end_date = allocation.end_date
        resource = allocation.queue.resource
        key = (allocation.project, resource)
        if key not in end_dates_tmp or end_date > end_dates_tmp[key]:
            end_dates_tmp[key] = end_date

    # We create the map: (user, resource) -> end_date
    end_dates = dict()
    for key, end_date in list(end_dates_tmp.items()):
        project, resource = key
        users = project.members.all()  # this probably uses a lot of eb requests
        for user in users:
            key = (user, resource)
            if key not in end_dates or end_date > end_dates[key]:
                end_dates[key] = end_date

    end_date_no_project = datetime.date.today() + extra_days

    # Apply the new end_dates:
    for account in Account.objects.exclude(unixnames_to_exclude).exclude(staff=True).prefetch_related('resource', 'user'):
        key = (account.user, account.resource)
        if key in end_dates:
            max_end_date = end_dates[key] + extra_days

            if max_end_date != account.end_date:
                # if options.force or (max_end_date - account.end_date) <= timedelta(days=6*30):
                if verbose:
                    print('Account: {0:10}@{1}, end_date: {2} -> {3}'.format(
                        account.unixname, account.resource.name, account.end_date, max_end_date))
                if not dry_run:
                    account.end_date = max_end_date
                    account.save()
        elif account.end_date > end_date_no_project:
            if verbose:
                print('Account: {0:10}@{1}, end_date: {2} -> {3}  (no active projects)'.format(
                    account.unixname, account.resource.name, account.end_date, end_date_no_project))
            if not dry_run:
                account.end_date = end_date_no_project
                account.save()


def import_user_expiry_date(dry_run=False, verbose=False):
    """
    Updates name, email. user agreement state.
    """
    supr = SUPR()

    # Search parameters
    params = {
        'modified_since': (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")}

    try:
        supr_persons = supr.get('/person/search/', params=params)
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        print("HTTP error %s from SUPR:" % e.status_code)
        print(e.text)
        raise

    for p in supr_persons.matches:

        if not p.centre_person_id:
            continue

        # Get User in Tengil
        try:
            u = User.objects.get(id=p.centre_person_id)
        except User.DoesNotExist:
            if verbose:
                print("** ERROR: User id=%s not found in Tengil (but id = centre_person_id in SUPR)" %
                      p.centre_person_id)
            continue

        import_object_expiry_date(p, u, dry_run, verbose)


def import_user_metadata(dry_run=False, verbose=False):
    """
    Updates name, email. user agreement state.
    """
    supr = SUPR()
    g = get_globals()

    # Search parameters
    params = {'modified_since': g.latest_person_update_from_supr}

    # Search for persons in SUPR modified since
    # g.latest_person_update_from_supr
    try:
        supr_persons = supr.get('/person/search/', params=params)
    except SUPRHTTPError as e:
        # We want to show the text received if we get an HTTP Error
        print("HTTP error %s from SUPR:" % e.status_code)
        print(e.text)
        raise

    for p in supr_persons.matches:

        if not p.centre_person_id:
            continue

        # Get User in Tengil
        try:
            u = User.objects.get(id=p.centre_person_id)
        except User.DoesNotExist:
            if verbose:
                print("** ERROR: User id=%s not found in Tengil (but id = centre_person_id in SUPR)" %
                      p.centre_person_id)
            continue

        if not u.SUPRID:
            if verbose:
                print("** ERROR: User %s (Tengil id=%s) not linked with SUPR" %
                      (u.fullName(), u.id))

        if u.SUPRID != p.id:
            if u.SUPRID in p.merged_ids:
                if verbose:
                    print("** INFO: User %s (Tengil id=%s) points to SUPR Person id=%s (has been merged into id=%s). Updating." % (
                        u.fullName(), u.id, u.SUPRID, p.id))
                u.SUPRID = p.id
                if verbose:
                    print("         User %s (Tengil id=%s) updated SUPRID to %s" % (
                        u.fullName(), u.id, u.SUPRID))
            else:
                if verbose:
                    print("** ERROR: User %s (Tengil id=%s) points to SUPR Person id=%s" %
                          (u.fullName(), u.id, u.SUPRID))

        if u.firstName != p.first_name:
            if verbose:
                print("** INFO: User %s (Tengil id=%s) first name (%s) changed to %s" % (
                    u.fullName(), u.id, u.firstName, p.first_name))
            u.firstName = p.first_name

        if u.lastName != p.last_name:
            if verbose:
                print("** INFO: User %s (Tengil id=%s) last name (%s) changed to %s" % (
                    u.fullName(), u.id, u.lastName, p.last_name))
            u.lastName = p.last_name

        if u.email != p.email:
            if verbose:
                print("** INFO: User %s (Tengil id=%s) email (%s) changed to %s" %
                      (u.fullName(), u.id, u.email, p.email))
            u.email = p.email

        if u.phone != p.tel1:
            if verbose:
                print("** INFO: User %s (Tengil id=%s) phone (%s) changed to %s" %
                      (u.fullName(), u.id, u.phone, p.tel1))
            u.phone = p.tel1

        # Has Person in SUPR accepted User Agreement?
        if not u.ua_accepted_in_supr:
            try:
                x = p.user_agreement_accepted
                u.ua_accepted_in_supr = x
            except KeyError:
                pass

        if not dry_run:
            u.save()

    # Updating timestamp
    if not dry_run:
        g = get_globals()
        g.latest_person_update_from_supr = supr_persons.began
        g.save()


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
    tengil_set = {(o.id, o.status) for o in tengil_accounts}
    update_accounts_in_supr = tengil_set - supr_set
    for openstack_id, status in update_accounts_in_supr:
        params = {"status": status}
        try:
            if verbose:
                print(
                    "Updated account {0}@{1} to status \"{2}\"".format(username, r.name, status))
            if not dry_run and settings.PRODUCTION:
                supr.post(
                    '/resource/{0}/account/{1}/update/'.format(r.suprid, username), params)
        except SUPRHTTPError as e:
            print("{0}: HTTP error {1} from SUPR: {2}".format(
                username, e.status_code, e.text))


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
