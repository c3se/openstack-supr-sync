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

    def get_domains(self):
        return self.connection.identity.domains()

    def get_services(self):
        return self.connection.identity.services()

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
