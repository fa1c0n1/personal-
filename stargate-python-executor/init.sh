#!/usr/bin/env bash
set -x

INIT_SCRIPT_DIR=$(dirname "$0")
cd $INIT_SCRIPT_DIR
SOURCEDIR=$PWD

if [[ -z "${STARGATE_VERSION}" ]]; then
  export STARGATE_VERSION=0.0.0
fi

if [[ ! -z "${STARGATE_PYTHON_VENV_PATH}" ]]; then
  echo "activating virtual env"
  cd $STARGATE_PYTHON_VENV_PATH
  [[ -L stargate-python-executor ]] && { unlink stargate-python-executor; }
  ln -s $SOURCEDIR stargate-python-executor
  chmod +x bin/activate
  source bin/activate
  cd stargate-python-executor
  python -m pip install --no-cache -i https://pypi.apple.com/simple --extra-index-url https://artifacts.apple.com/api/pypi/apple-pypi-integration-local/simple --trusted-host=pypi.apple.com -r requirements.txt
  [[ -z "$APP_HOME" ]] && { export APP_HOME=$SOURCEDIR; }
  [[ -z "$APP_MODE" ]] && { export APP_MODE=DEV; }
  mkdir -p $APP_HOME/logs
  [[ -z "$STARGATE_CONTAINER_PORT" ]] && { export STARGATE_CONTAINER_PORT=8878; }
  [[ -z "$STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE" ]] && { export STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE=10; }
  [[ -z "$STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT" ]] && { export STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT="localhost:8808"; }
else
  if [[ -z "${STARGATE_USE_SUPPLIED_PYTHON_RUNNER}" ]]; then
    cd pkg
    python -m pip install --no-index --find-links . -r ../requirements.txt
    cd ..
  fi
fi

init_stargate_python_runner
