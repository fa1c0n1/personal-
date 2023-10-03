#!/bin/bash
set -e
mkdir -p /opt/stargate/logs
touch /opt/stargate/logs/stargate-service-error.log
exec 2>/opt/stargate/logs/stargate-service-error.log
exec "$@"
