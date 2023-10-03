from stargate.gateway.server import start_server
from stargate.util.logger import set_logging


def init_runner():
    set_logging()
    start_server()
