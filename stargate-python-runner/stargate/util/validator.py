import json
import logging
import pkgutil

from jsonschema import validate

logger = logging.getLogger(__name__)

init_schema = pkgutil.get_data(__name__, 'jsonschema/python_func_init_schema.json').decode('utf-8')
exe_schema = pkgutil.get_data(__name__, 'jsonschema/python_func_call_schema.json').decode('utf-8')
jinja_schema = pkgutil.get_data(__name__, 'jsonschema/python_func_call_schema.json').decode('utf-8')


def validateFunctionInitRequest(request: dict):
    validate(instance=request, schema=json.loads(init_schema))


def validateFunctionExecutionRequest(request: dict):
    validate(instance=request, schema=json.loads(exe_schema))


def validateJinjaExecutionRequest(request: dict):
    validate(instance=request, schema=json.loads(jinja_schema))
