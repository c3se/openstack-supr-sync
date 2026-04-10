import logging
import smtplib

from openstack_supr_sync.config import config

from email.message import EmailMessage
from email.policy import SMTP
from email.utils import make_msgid
from email.utils import formatdate

email_config = config['email']
logger = logging.getLogger(__name__)

plaintext_template = """
Dear {name},

an account with username {username} has been created for you on
Cirrus @ C3SE. You can log in to it via {url}, selecting
"Authenticate via c3se keycloak" from the drop-down list.
You do not need a password, as you will authenticate via SUPR.

For questions, please use the SUPR support form at {support_url}.
"""

def send_email(message: EmailMessage):
    with smtplib.SMTP(email_config['smtp_server'],
                      local_hostname=email_config['local_hostname']) as server:
        server.set_debuglevel(1)
        logger.info('Sending message...')
        server.send_message(message)

def send_account_email(**content):
    email_msg = EmailMessage(policy=SMTP)
    for k, v in email_config['headers'].items():
        if k == 'subject':
            v = v.format(username=content['username'])
        email_msg[k] = v
    email_msg['To'] = content['To']
    content['url'] = email_config['url']
    content['support_url'] = email_config['support_url']
    email_msg.set_content(plaintext_template.format(**content))
    send_email(email_msg) 
