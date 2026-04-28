""" Draft of class for getting resources """
import openstack
import logging
from openstack_supr_sync.config import config
network_config = config['network']
quota_config = config['quota']

logger = logging.getLogger(__name__)

class OpenstackObjects:
    def __init__(self, cloud):
        self.cloud = cloud
        self._connection = openstack.connect(cloud)

    @property
    def connection(self):
        return self._connection

    @property
    def member(self):
        return self.connection.identity.find_role('member')

    def get_projects(self):
        return self.connection.identity.projects()

    def get_users(self):
        return self.connection.identity.users()

    def get_servers(self):
        return self.connection.compute.servers(all_projects=True)

    def get_volumes(self):
        return self.connection.volume.volumes(all_projects=True)

    def get_snapshots(self):
        return self.connection.volume.snapshots(all_projects=True)

    def get_vm_snapshots(self):
        images = self.connection.compute.images()
        return [i for i in images if i.metadata.get('image_type', '') == 'snapshot']
    
    def get_backups(self):
        return self.connection.volume.backups(all_projects=True)

    def get_domains(self):
        return self.connection.identity.domains()

    def get_services(self):
        return self.connection.identity.services()

    def get_instances(self, project_id):
        if project_id is None:
            return self.connection.compute.servers(all_projects=True)
        return self.connection.compute.servers(all_projects=True, project_id=project_id)

    def set_project_storage_quota(self,
                                  project_id,
                                  storage_in_gb=None,
                                  number_of_volumes=None,
                                  number_of_snapshots=None,
                                  number_of_backups=None):
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
        if number_of_backups is not None:
            kwargs['backups'] = number_of_backups
        return self.connection.block_storage.update_quota_set(
            project_id, **kwargs)

    def set_default_network_quota(self, project_id):
        network_quota = quota_config['network']
        self.set_project_network_quota(project_id, **network_quota)

    def set_project_network_quota(
            self,
            project_id,
            floating_ips=None,
            security_groups=None,
            security_group_rules=None,
            networks=None,
            ports=None,
            routers=None):
        kwargs = {}
        if floating_ips is not None:
            kwargs['floating_ips'] = floating_ips
        if security_groups is not None:
            kwargs['security_groups'] = security_groups
        if security_group_rules is not None:
            kwargs['security_group_rules'] = security_group_rules
        if networks is not None:
            kwargs['networks'] = networks
        if ports is not None:
            kwargs['ports'] = ports
        if routers is not None:
            kwargs['routers'] = routers
        return self.connection.network.update_quota(
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

    def get_user(self, user):
        return self.connection.get_user(user)

    def update_user(self, user, **kwargs):
        user = self.get_user(user)
        return self.connection.identity.update_user(user, **kwargs)

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
        return self.connection.identity.get_project(project_id)

    def get_project_members(self, project):
        user_ids = [c['user']['id']
                    for c in self.connection.identity.role_assignments(
                    scope=dict(project=dict(id=project.id)))]
        user_names = [self.connection.identity.get_user(uid)['name'] for uid in user_ids]
        return user_names

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

    def set_project_quota(self, project, quota):
        return self.connection.set_compute_quotas(project, **quota)

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

    def make_router_for_project(self, project_id):
        """
        Sets up an internal network with an external gateway for the project.
        """
        project = self.get_project(project_id)
        network_name = f'{project.name} IPv4 Network'
        subnet_name = f'{project.name} IPv4 Subnet'
        router_name = f'{project.name} IPv4 Router'
        external_gateway = self.connection.network.find_network(network_config['external_network'])
        eg_subnet = list(self.connection.network.subnets(network_id=external_gateway.id))[0]
        network = self.connection.network.create_network(name=network_name, project_id=project_id)
        subnet = self.connection.network.create_subnet(
                name=subnet_name,
                network_id=network.id,
                ip_version=4,
                dns_nameservers=[network_config['external_dns']],
                gateway_ip=network_config['internal_gateway'],
                cidr=network_config['internal_cidr'],
                project_id=project_id)
        gateway_info = dict(network_id=external_gateway.id, enable_snat=True)
        router = self.connection.network.create_router(name=router_name, project_id=project_id, external_gateway_info=gateway_info)
        self.connection.network.add_interface_to_router(router=router, subnet_id=subnet.id)

    def delete_user(self, user_id):
        logger.info("Deleting user {user_id}")
        self.connection.identity.delete_user(user_id)

    def delete_project_with_cleanup(self, project_id, force=False):
        """ Deletes a project and cleans up associated resources like
        networks, instances, etc. 

        
        project_id
            ID of the project to be deleted.
        force
            If True, uses the "--force" option to delete instances.
            Default is false.
        """
        logger.info(f'Cleaning up project {project_id}')
        self.delete_project_instances(project_id, force=force)
        self.delete_project_networks(project_id)
        logger.info(f'Deleting project {project_id}')
        self.connection.identity.delete_project(project_id)

    def delete_project_instances(self, project_id, force=False):
        """ Deletes instances attached to project """
        instances = self.get_instances(project_id)
        logger.info(f'Cleaning up instances of {project_id}')
        for ii in instances:
            logger.info(f'Deleting instance {ii.id}')
            self.connection.compute.delete_server(ii, force=force)

    def delete_project_networks(self, project_id):
        """ Deletes project and cleans up any network objects. """
        logger.info(f'Cleaning up IP addresses of {project_id}')
        ips = self.connection.network.ips(project_id=project_id)
        for ip in ips:
            self.connection.network.delete_ip(ip)
        ports = self.connection.network.ports(project_id=project_id)
        logger.info(f'Cleaning up ports of {project_id}')
        for pt in ports:
            if pt.device_owner != 'network:dhcp':
                for fip in pt.fixed_ips:
                    try:
                        self.connection.network.remove_interface_from_router(pt.device_id, fip['subnet_id'], pt.id)
                    except Exception as e:
                        logger.info(f'Could not delete interface  ({pt.device_id}, {fip["subnet_id"]}, {pt.id}) due to:\n'
                                    f'{e}')
                        pass
            self.connection.network.delete_port(pt.id)
        routers = self.connection.network.routers(project_id=project_id)
        logger.info(f'Cleaning up routers of {project_id}')
        for rt in routers:
            self.connection.network.delete_router(rt.id)
        subnets = self.connection.network.subnets(project_id=project_id)
        logger.info(f'Cleaning up subnets of {project_id}')
        for sn in subnets:
            self.connection.network.delete_subnet(sn.id)
        networks = self.connection.network.networks(project_id=project_id)
        logger.info(f'Cleaning up networks of {project_id}')
        for nw in networks:
            logger.info(f'Deleting network {nw.id}')
            self.connection.network.delete_network(nw.id)
