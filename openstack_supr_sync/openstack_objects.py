""" Draft of class for getting resources """
import openstack
import config
from .connection_manager import ConnectionManager


class OpenstackObjects:
    def __init__(self, connection_manager=ConnectionManager):
        self._connection_manager = connection_manager

    @property
    def connection(self):
        return self._connection_manager.connection

    def get_projects(self):
        return self.connection.identity.projects()

    def get_users(self):
        return self.connection.identity.users()

    def get_domains(self):
        return self.connection.identity.domains()

    def get_services(self):
        return self.connection.identity.services()

    def find_user(self, name, user_id=None, filters=None, domain_id=None):
        if id is not None:
            return self.connection.identity.get_user_by_id(user_id)
        else:
            return self.connection.identity.get_user(name, filters, domain_id)

    def update_user(self, id, **kwargs):
        return self.connection.identity.update_user(id, **kwargs)

    def create_user(self, name, **kwargs):
        return self.connection.identity.create_user(name, **kwargs)

   # and so on for projects, etc, groups
