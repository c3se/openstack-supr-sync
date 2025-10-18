import logging
import signal


logger = logging.getLogger(__name__)


class SignalHandler:
    def __init__(self):
        self._shutdown_requested = False
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        logger.info('Request to shutdown received, stopping')
        self._shutdown_requested = True

    @property
    def shutdown_requested(self):
        return self._shutdown_requested
