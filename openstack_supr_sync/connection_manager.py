""" Draft of connection manager """
import openstack


class ConnectionManager():
    def __init__(self, cloud):
        self.cloud = cloud
        self._connection = openstack.connect(cloud)

    @property
    def connection(self):
        # do some logic here to make sure the connection is alive?
        return self._connection
