import apple_certifi
import grpc


def create_grpc_channel(target, secure=True):
    if secure:
        path_to_certs = apple_certifi.where()
        with open(path_to_certs, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
            return grpc.secure_channel(target=target, credentials=credentials)
    else:
        return grpc.insecure_channel(target=target, options=(('grpc.enable_http_proxy', 0),))

# If we need to create utils for managing pool of GRPC channels in future, that should go here.
