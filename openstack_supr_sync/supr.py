import requests
import simplejson
from .config import config, secrets

secrets = secrets['supr']
config = config['supr']


# Our own exceptions
class SUPRException(Exception):
    pass


class SUPRHTTPError(SUPRException):
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def __str__(self):
        if self.status_code > 0:
            return "SUPRHTTPError(%d)" % self.status_code
        else:
            return "SUPRHTTPError - " + self.text


class SUPRBadJSON(SUPRException):
    pass


# We want a dict with a twist: the ability to access it using
# attribute notation (X.key) as an alternative to indexing notation
# (X["key"]).  The main caveat is that this does not work for keys that
# have the same name as a dict method/attribute. If we have a key
# named "keys" in the dict, X["keys"] will get that, while X.keys will
# get the keys dict method. When in doubt, use normal indexing.
class SUPRdict(dict):
    def __getattr__(self, name):
        return self[name]


SUPRDecoder = simplejson.JSONDecoder(object_hook=lambda x: SUPRdict(x))


class SUPR(object):
    def __init__(self):
        self.base_url = config['api_base_url']
        self.user = secrets['api_user']
        self.password = secrets['api_key']

    def get(self, url, params=None):
        url = self.base_url + url

        try:
            r = requests.get(url,
                             auth=(self.user, self.password),
                             params=params)
        except Exception as e:
            raise SUPRHTTPError(0, str(e))

        if r.status_code == 200:
            try:
                decoded_data = SUPRDecoder.decode(r.content)
            except Exception:
                raise SUPRBadJSON
            return decoded_data
        else:
            raise SUPRHTTPError(r.status_code, r.text)

    def post(self, url, data):
        url = self.base_url + url
        try:
            encoded_data = simplejson.dumps(data)
        except Exception:
            raise SUPRBadJSON

        r = requests.post(url,
                          auth=(self.user, self.password),
                          data=encoded_data)

        if r.status_code == 200:
            try:
                decoded_data = SUPRDecoder.decode(r.content)
            except Exception:
                raise SUPRBadJSON
            return decoded_data
        else:
            raise SUPRHTTPError(r.status_code, r.text)
