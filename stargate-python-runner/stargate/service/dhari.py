import json
import logging
import time
import traceback
import uuid

from stargate.api.service_api import DhariService
from stargate.options.service_options import DhariOptions

from stargate.rpc.proto import dhari_pb2_grpc
from stargate.rpc.proto.dhari_pb2 import DhariPing, JsonPayloadWithHeaders
from stargate.service.resource_manager_client import ResourceManagerGrpcClient
from stargate.util.grpc_utils import create_grpc_channel

logger = logging.getLogger(__name__)


# Note: We are not using streaming RPC in Dhari. So creating a pool of gRPC channels
# may not be necessarily. The unary GRPC calls are sent over single channel (a channel
# has more than one HTTP/2 connections). If needed, pool can be added later based on
# performance characteristics.

class DhariServiceImpl(DhariService):
    """The Dhari ingestion service talking directly to the Dhari GRPC server."""

    DEFAULT_UNSTRUCTURED_FIELD_NAME = 'unstructured'
    DHARI_APP_ID = 172117

    def __init__(self, node_name: str, options: DhariOptions) -> None:
        self.node_name = node_name
        self.options = options
        self.stub = dhari_pb2_grpc.DhariProtoServiceStub(create_grpc_channel(options.grpc_endpoint))
        ping_status = self.stub.ping(DhariPing(time=time.time_ns()))
        if ping_status is not None and ping_status.status:
            self.resource_client = ResourceManagerGrpcClient()
            logger.info(f"Dhari service initialized successfully for node {self.node_name}")  # Not logging out
            # options, might have credentials
        else:
            raise Exception("Error initializing dhari service. Dhari ping status failed. Existing..")

    def ingest(self, payload: object, schema_id: str = None, payload_key: str = None, publisher_id: str = None,
               headers: dict = None) -> dict:
        return self._ingest(payload, schema_id, payload_key, publisher_id, headers, False)

    def async_ingest(self, payload: object, schema_id: str = None, payload_key: str = None, publisher_id: str = None,
                     headers: dict = None) -> dict:
        return self._ingest(payload, schema_id, payload_key, publisher_id, headers, True)

    def _ingest(self, payload, schema_id, payload_key, publisher_id, headers, use_async):
        payload_key = payload_key or str(uuid.uuid4())
        token = self.resource_client.get_token(
            source_app_id=int(self.options.a3_app_id),
            target_app_id=DhariServiceImpl.DHARI_APP_ID,
            password=self.options.a3_password,
            context_string=self.options.a3_context_string
        )
        json_payload = JsonPayloadWithHeaders(
            appId=int(self.options.a3_app_id),
            token=token,
            publisherId=publisher_id or self.options.publisher_id,
            schemaId=schema_id or self.options.schema_id,
            payloadKey=payload_key,
            payload=json.dumps(payload),
            headers=headers,
            unstructuredFieldName=DhariServiceImpl.DEFAULT_UNSTRUCTURED_FIELD_NAME
        )

        try:
            if use_async:
                response = self.stub.ingestPayloadAsync(json_payload)
            else:
                response = self.stub.ingestPayload(json_payload)
        except Exception as e:
            logger.error(f'Payload could not be ingested - {traceback.format_exc()}')
            raise Exception(e)

        if response.count <= 0:
            logger.error(f'Received message process count {response.count} indicating that payload could not be '
                         f'ingested - {response.exception}')
            raise Exception(response.exception)

        return {"count": response.count, "response": json.dumps(json.loads(response.responseJson)), "payloadKey": payload_key}
