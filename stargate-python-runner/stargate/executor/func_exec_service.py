import json
import logging
import os
import sys
import threading
import traceback
from functools import wraps
from importlib import util

import jinja2
import prometheus_client

from stargate.rpc.proto import lambda_pb2_grpc, lambda_pb2
from stargate.service.service_provider_mixin import ServiceProviderMixinImpl
from stargate.util.logger import set_logging, set_log_headers
from stargate.util.platform_utils import create_transform_with_provided_services

logger = logging.getLogger(__name__)

node_to_function_map = {}
node_to_template_map = {}
templateEnv = None
EMPTY_STRING = ""
MISSING_INIT_CONFIG_ERROR = "MISSING_INIT_CONFIG_ERROR"


def synchronized(function):
    lock = threading.RLock()

    @wraps(function)
    def _wrapper(*args, **kwargs):
        with lock:
            return function(*args, **kwargs)

    return _wrapper


def set_log_header_values(node_name: str, log_context: dict = {}):
    log_headers_dict = log_context
    log_headers_dict['nodeName'] = node_name
    set_log_headers(log_headers_dict)


class FunctionExecutorService(lambda_pb2_grpc.ExternalFunctionProtoService):

    @synchronized
    def init(self, request,
             target,
             options=(),
             channel_credentials=None,
             call_credentials=None,
             insecure=False,
             compression=None,
             wait_for_ready=None,
             timeout=None,
             metadata=None):

        init_request: lambda_pb2.InitPayload = request

        if init_request.nodeName not in node_to_function_map and init_request.nodeName not in node_to_template_map:

            try:
                set_log_header_values(init_request.nodeName)

                # getting the node_config for further processing
                node_config = json.loads(init_request.nodeConfig)

                py_file = node_config['functionOptions'].get('file')
                py_module = node_config['functionOptions'].get('module') \
                    if node_config['functionOptions'].get('module') is not None else \
                    os.path.splitext(os.path.basename(py_file))[0]
                py_class = node_config['functionOptions'].get('class')

                logger.info(f'Python Init function Options -'
                            f' File : {py_file}'
                            f' Module : {py_module}'
                            f' Class : {py_class}')

                # updating the sys.path for looking up the modules depending on the file location
                for top, dirs, files in os.walk(os.path.splitext(os.path.dirname(py_file))[0]):
                    sys.path.append(top)

                # load the class
                spec = util.spec_from_file_location(name=py_module, location=py_file)
                module = util.module_from_spec(spec)
                if py_module not in sys.modules:
                    sys.modules[py_module] = module
                spec.loader.exec_module(module)

                instance_create_status, exceptions, instance = create_transform_with_provided_services(
                    transform_class=getattr(module, py_class),
                    provider_class=ServiceProviderMixinImpl,
                    provider_options_dict=node_config,
                    context=node_config['context'])

                set_logging()

                # if object creation is success then save it for future reference
                if not exceptions and instance_create_status:
                    # if not template
                    if not node_config['isTemplate']:
                        node_to_function_map[init_request.nodeName] = (instance, "applyWithKey")
                    else:
                        node_to_template_map[init_request.nodeName] = {
                            'template': node_config['template']
                        }

                        global templateEnv
                        templateEnv = jinja2.Environment(autoescape=True)

                    return lambda_pb2.InitStatus(
                        status=True,
                        message=f"{init_request.nodeName} node init successful")
                else:
                    logger.error(f"Error while creating instance for lambda class {py_class} - {exceptions}")
                    return lambda_pb2.InitStatus(
                        status=False,
                        message=exceptions)

            except Exception as e:
                logger.error(
                    f"Error processing init request for node {init_request.nodeName} - {traceback.format_exc()}")
                return lambda_pb2.InitStatus(
                    status=False,
                    message=str(e))

        else:
            logger.warning(f"Init request received for already registered node {init_request.nodeName}... Ignoring")
            return lambda_pb2.InitStatus(
                status=True,
                message=f"{init_request.nodeName} node already initialized!")

    def apply(self, request,
              target,
              options=(),
              channel_credentials=None,
              call_credentials=None,
              insecure=False,
              compression=None,
              wait_for_ready=None,
              timeout=None,
              metadata=None):

        execute_request: lambda_pb2.RequestPayload = request
        set_log_header_values(execute_request.nodeName, execute_request.logContext)

        # check if its function or template execution
        if execute_request.nodeName in node_to_function_map:

            try:
                instance, method = node_to_function_map[execute_request.nodeName]

                exec_method = getattr(instance, method)
                result = exec_method(json.loads(execute_request.payload), execute_request.payloadKey)

                if result:
                    return lambda_pb2.ResponsePayload(
                        response=json.dumps(result),
                        skip=False,
                        exception=EMPTY_STRING,
                        stackTrace=EMPTY_STRING
                    )
                else:
                    return lambda_pb2.ResponsePayload(
                        response=EMPTY_STRING,
                        skip=True,
                        exception=EMPTY_STRING,
                        stackTrace=EMPTY_STRING
                    )

            except Exception as e:
                stack_trace = traceback.format_exc()
                logger.error(f"Error processing apply request for node {execute_request.nodeName} - {stack_trace}")
                return lambda_pb2.ResponsePayload(
                    response=EMPTY_STRING,
                    skip=False,
                    exception=str(e),
                    stackTrace=stack_trace)

        elif execute_request.nodeName in node_to_template_map:
            template_config = node_to_template_map[execute_request.nodeName]
            logger.info(f"retrieved template_config - {template_config.__str__()}")
            template = templateEnv.from_string(template_config['template'])

            template_variables = json.loads(execute_request.payload)
            try:
                return lambda_pb2.ResponsePayload(
                    response=template.render(template_variables),
                    skip=False,
                    exception=EMPTY_STRING,
                    stackTrace=EMPTY_STRING)
            except Exception as e:
                stack_trace = traceback.format_exc()
                logger.error(f"Error processing apply request for node {execute_request.nodeName} - {stack_trace}")
                return lambda_pb2.ResponsePayload(
                    response=EMPTY_STRING,
                    skip=False,
                    exception=str(e),
                    stackTrace=stack_trace)
        else:
            error_msg = f"Invalid node {execute_request.nodeName}... No config found to execute"
            logger.error(error_msg)
            return lambda_pb2.ResponsePayload(
                response=EMPTY_STRING,
                skip=False,
                exception=MISSING_INIT_CONFIG_ERROR,
                stackTrace=error_msg)

    def metrics(self, request,
                target,
                options=(),
                channel_credentials=None,
                call_credentials=None,
                insecure=False,
                compression=None,
                wait_for_ready=None,
                timeout=None,
                metadata=None):

        return lambda_pb2.MetricsResponse(
            response=prometheus_client.generate_latest().decode('utf-8')
        )
