""" Draft of class for getting resources """
from .connection_manager import ConnectionManager


class OpenstackObjects:
    def __init__(self, connection_manager=ConnectionManager):
        self._connection_manager = connection_manager

    @property
    def connection(self):
        return self._connection_manager.connection

    @property
    def member(self):
        return self.connection.identity.find_role('member')

    def get_projects(self):
        return self.connection.identity.projects()

    def get_users(self):
        return self.connection.identity.users()

    def get_servers(self):
        return self.connection.compute.servers(all_projects=True)

    def get_domains(self):
        return self.connection.identity.domains()

    def get_services(self):
        return self.connection.identity.services()

    def set_project_storage_quota(self,
                                  project_id,
                                  storage_in_gb=None,
                                  number_of_volumes=None,
                                  number_of_snapshots=None):
        """
        Sets the 'gigabytes' storage quota which modifies the total size
        of volumes and volume snapshots, as well as the number of volumes nad snaphsots.

        If any one value is not set, it is left unchanged.
        """
        kwargs = {}
        if storage_in_gb is not None:
            kwargs['gigabytes'] = storage_in_gb
        if number_of_volumes is not None:
            kwargs['volumes'] = number_of_volumes
        if number_of_snapshots is not None:
            kwargs['snapshots'] = number_of_snapshots
        return self.connection.block_storage.update_quota_set(
            project_id, **kwargs)

    def set_project_vm_quota(self,
                             project_id,
                             cores=None,
                             ram=None):
        """
        Set the number of vCPUs (cores) and RAM a project can use. If `cores` and `ram`
        are not set when called, they are unmodified.
        """
        kwargs = {}
        if cores is not None:
            kwargs['cores'] = cores
        if ram is not None:
            kwargs['ram'] = ram
        return self.connection.compute.update_quota_set(
            project_id, **kwargs)

    def get_user(self, user_id):
        return self.connection.identity.get_user_by_id(user_id)

    def find_user(self, name=None, user_id=None, filters=None, domain_id=None):
        if user_id is not None:
            self.get_user(user_id)
        else:
            return self.connection.identity.get_user(name, filters, domain_id)

    def update_user(self, project_id, **kwargs):
        return self.connection.identity.update_user(project_id, **kwargs)

    def create_user(self, name, **kwargs):
        """
        kwargs
            Description

            enabled
        """
        kwargs['name'] = name
        return self.connection.identity.create_user(**kwargs)

    def find_project(self, name=None, project_id=None, filters=None, domain_id=None):
        if id is not None:
            return self.get_project(project_id)
        else:
            return self.connection.identity.get_project(name)

    def get_project(self, project_id):
        return self.connection.identity.get_project_by_id(project_id)

    def update_project(self, project_id, **kwargs):
        return self.connection.identity.update_project(project_id, **kwargs)

    def create_project(self, name, **kwargs):
        """
        kwargs
            Description

            enabled

            is_domain
        """
        kwargs['name'] = name
        return self.connection.identity.create_project(**kwargs)

    def add_user_to_project(self, project_id, user_id):
        """
        Add a user to a project as a `member`.
        """
        return self.connection.identity.assign_project_role_to_user(
            project_id, user_id, self.member)

    def remove_user_from_project(self, project_id, user_id):
        """
        Add a user to a project as a `member`.
        """
        return self.connection.identity.unassign_project_role_from_user(
            project_id, user_id, self.member)
