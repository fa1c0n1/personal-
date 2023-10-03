import logging
import os
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

logger_set = set()
APP_HOME = os.getenv("APP_HOME", "/tmp")

log_headers = ContextVar('log_headers', default=None)


def set_log_prefix() -> str:
    app_id = os.getenv("APP_ID") if os.getenv("APP_ID") is not None else "172847"
    additional_prefix = os.getenv("APP_LOG_ADDITIONAL_PREFIX") if os.getenv("APP_LOG_ADDITIONAL_PREFIX") is not None \
        else ",\"pipelineId\":\"DEFAULT\", \"runNo\":\"1\", \"hostType\":\"worker\", \"container\" : \"python\""
    app_log_name = os.getenv("APP_LOG_NAME") if os.getenv("APP_LOG_NAME") is not None else "stargate-flink-executor"

    prefix = f"\"appid\":\"{app_id}\", \"appName\":\"{app_log_name}\" {additional_prefix}"
    return prefix


def rotated_file_name(default_name: str) -> str:
    name, ext, num = default_name.split(".")
    return f"{name}.{num}.{ext}"


def set_log_headers(req_log_headers: dict):
    global log_headers
    log_headers.set(', '.join(
        "\"{}\": \"{}\"".format(k, v) for k, v in req_log_headers.items()))


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        global log_headers
        record.prefix = set_log_prefix() if not log_headers.get() else ', '.join(
            [set_log_prefix(), log_headers.get()])
        record.msg = "\r".join(str(record.msg).splitlines())
        return True


# TODO - Probably move all the constants to config file and use typesafe config to read the executor specific config
#  in general
def set_logging():
    l_filter = ContextFilter()

    formatter = logging.Formatter(
        '{"timestamp":"%(asctime)s.%(msecs)03dZ", %(prefix)s, "level":"%(levelname)s", "logger": "%(name)s", '
        '"msg":"%(message)s"}',
        datefmt='%Y-%m-%dT%H:%M:%S')

    file_name = f"{APP_HOME}/logs/stargate-python-executor.log"
    l_handler = RotatingFileHandler(filename=file_name,
                                    maxBytes=10000000,
                                    backupCount=10)
    l_handler.namer = rotated_file_name
    l_console_handler = logging.StreamHandler()
    l_handler.setFormatter(formatter)
    l_console_handler.setFormatter(formatter)
    l_handler.addFilter(l_filter)
    l_console_handler.addFilter(l_filter)

    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        if logger not in logger_set:
            logger.handlers = []
            logger.propagate = False
            logger.addHandler(l_handler)
            logger.addHandler(l_console_handler)
            logger_set.add(logger)

    logging.getLogger().setLevel(logging.INFO)
