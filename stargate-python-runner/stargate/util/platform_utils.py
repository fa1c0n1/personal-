import logging
import traceback
from copy import deepcopy
from typing import Dict, Type

from stargate.api.service_api import StargateServiceProviderMixin
from stargate.api.transform_api import Transform
from stargate.options.service_options import ServiceProviderMixinOptions

logger = logging.getLogger(__name__)


def create_transform_with_provided_services(
        transform_class: Type[Transform],
        provider_class: Type[StargateServiceProviderMixin],
        provider_options_dict: Dict[str, object],
        context: Dict[str, object] = None
):
    '''This is a convenience function to do the dependency injection. It takes the
    user's Transform class, platform provided ServiceProviderMixin class, the
    options/config to initialize the services, and the "context" to be injected.
    Returns an instance of the Transform with provided service injected into it.

    The technique used for dependency injection is based on Python's language feature of
    multiple inheritance and MRO, using Mixin classes.

    Because this function is expected to be called from deep inside the framework, for
    convenience, it doesn't directly propagate the error (if any) to the caller.
    Rather, it responds back with success/failure indicator/traceback, and
    the Transform object if the construction was successful. Return value is
    a tuple of (ok: bool, trace_back: str, transform: Transform).
    '''

    class TransformWithProvidedServices(transform_class, provider_class):
        def __init__(self, provider_options: ServiceProviderMixinOptions, context: Dict[str, object]) -> None:
            self._init_services(provider_options)
            self._set_context(context)
            super().__init__()

    try:
        provider_options = ServiceProviderMixinOptions.from_dict(deepcopy(provider_options_dict))
        context = deepcopy(context)
        instance = TransformWithProvidedServices(provider_options, context)
        return True, None, instance
    except Exception as e:
        return False, traceback.format_exc(), None
