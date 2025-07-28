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

    def get_user(self, user_id=None):
        return self.connection.identity.get_user_by_id(user_id)

    def find_user(self, name=None, user_id=None, filters=None, domain_id=None):
        if id is not None:
            self.get_user(user_id)
        else:
            return self.connection.identity.get_user(name, filters, domain_id)

    def update_user(self, id, **kwargs):
        return self.connection.identity.update_user(id, **kwargs)


    # Maybe this should take SUPRID and username and then automagically do the database wrangling.
    # At what stage do we generate a username? How?
    def create_user(self, name, **kwargs):
        """
        kwargs
            Description

            enabled
        """
        return self.connection.identity.create_user(name, **kwargs)

    def find_project(self, name=None, project_id=None, filters=None, domain_id=None):
        if id is not None:
            return self.get_project(project_id)
        else:
            return self.connection.identity.get_project(name)

    def get_project(self, project_id):
        return self.connection.identity.get_project_by_id(project_id)

    def update_project(self, id, **kwargs):
        return self.connection.identity.update_project(id, **kwargs)

    # As with users, do we automate the database reconciliation at this step?
    def create_project(self, name, **kwargs):
        """
        kwargs
            Description

            enabled

            is_domain
        """
        kwargs['name'] = name
        return self.connection.identity.create_project(**kwargs)

    # Do we make this work with SUPRID:s, or do we make another class with this abstraction layer?
    def add_user_to_project(self, project, user):
        """
        Add a user to a project as a `member`.

        Note that "project" and "user" must both be OpenStack Ids
        """
        return self.connection.identity.assign_project_role_to_user(
            project, user, self.member)
