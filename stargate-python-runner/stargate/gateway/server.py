import logging
import os
from concurrent import futures
from importlib.metadata import version

import grpc

from stargate.executor.func_exec_service import FunctionExecutorService
from stargate.rpc.proto import lambda_pb2_grpc

logger = logging.getLogger(__name__)


def start_server():
    logger.info('GRPC Python Server - Starting Server')
    server_port = int(os.getenv('STARGATE_CONTAINER_PORT', "8878"))
    grpc_pool_size = int(os.getenv('STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE', "10"))
    logger.debug(f'GRPC Python Server - GRPC Python Server Port found in environment, using port: "{server_port}" max_workers: "${grpc_pool_size}"')

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=grpc_pool_size))
    lambda_pb2_grpc.add_ExternalFunctionProtoServiceServicer_to_server(
        FunctionExecutorService(), server)
    server.add_insecure_port(f"[::]:{server_port}")
    server.start()
    logger.info("GRPC Python Server - Started Successfully")
    py_runner_version = version('stargate_python_runner')
    stargate_version = os.getenv('STARGATE_VERSION')
    if py_runner_version != stargate_version:
        logger.warning(f"Stargate python runner version {py_runner_version} doesn't match "
                       f"with Stargate version {stargate_version}.. Please update Stargate "
                       "python runner version at the earliest...")
    server.wait_for_termination()
