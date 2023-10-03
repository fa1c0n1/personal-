from stargate.api.service_api import StargateServiceProviderMixin
from stargate.options.service_options import ServiceProviderMixinOptions

from stargate.service.attributes import AttributesServiceImpl
from stargate.service.lookup import LookupServiceImpl
from stargate.service.dhari import DhariServiceImpl
from stargate.service.prometheus import PrometheusServiceImpl
from stargate.service.shuri_ofs import ShuriOfsServiceImpl


class ServiceProviderMixinImpl(StargateServiceProviderMixin):
    """Service Provider Mixin implementation used by Stargate Platform."""

    def _init_services(self, options: ServiceProviderMixinOptions) -> None:

        self._stargate_services = {}

        if options.attributes_options is not None:
            self._stargate_services[StargateServiceProviderMixin._service_enum.ATTRIBUTES] = AttributesServiceImpl(
                options.node_name, options.attributes_options)

        if options.lookup_options is not None:
            self._stargate_services[StargateServiceProviderMixin._service_enum.LOOKUP] = LookupServiceImpl(
                options.node_name, options.lookup_options)

        if options.dhari_options is not None:
            self._stargate_services[StargateServiceProviderMixin._service_enum.DHARI] = DhariServiceImpl(
                options.node_name, options.dhari_options)

        if options.shuri_ofs_options is not None:
            self._stargate_services[StargateServiceProviderMixin._service_enum.SHURI_OFS] = ShuriOfsServiceImpl(
                options.node_name, options.shuri_ofs_options)

        self._stargate_services[StargateServiceProviderMixin._service_enum.PROMETHEUS] = PrometheusServiceImpl(
            options.node_name)

        # setting up dhari service in case if it is for asynch lookup writes
        if StargateServiceProviderMixin._service_enum.LOOKUP in self._stargate_services:
            self._stargate_services.get(StargateServiceProviderMixin._service_enum.LOOKUP).dhari_service = \
                self._stargate_services.get(StargateServiceProviderMixin._service_enum.DHARI, None)
