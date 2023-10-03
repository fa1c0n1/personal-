import logging

logger = logging.getLogger(__name__)


class InvalidInputError(Exception):
    """Exception class for invalid input"""

    def __init__(self, message="Invalid input provided.."):
        self.message = message
        super().__init__(self.message)


class AttributesServiceException(Exception):
    """Exception on client side for Attributes Service"""
    pass


class LookupServiceException(Exception):
    """Exception on client side for Lookup Service"""
    pass
