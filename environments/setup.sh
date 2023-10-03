#!/bin/bash
set -e
set -o pipefail

if [[ -z "$1" ]] || [[ -z "$2" ]] || [[ -z "$3" ]]; then
  echo "usage: $0 <cluster> <namespace> <environment>"
  echo "sh setup.sh usmsc1 aml-shuri-dev dev"
  exit 1
fi

CLUSTER="$1"
NAMESPACE="$2"
ENVIRONMENT="$3"
PURGE_DEPLOYMENT="$4"
APPNAME="stargate"
ARTIFACTORY_ROOT_URL=https://artifacts.apple.com/aml-kairos-release-local/com/apple/aml/kairos/shuri/helm-charts/$APPNAME
export ARTIFACTORY_API_KEY=$(cat $BUILD_SECRETS_PATH/ARTIFACTORY_API_KEY)

function purge_deployment() {
  helm tiller run "${NAMESPACE}" -- helm delete ${APPNAME} --purge
}

function kubeconfig_setup() {
  mkdir -p ~/.kube
  cp "$BUILD_SECRETS_PATH/kubeconfig-${CLUSTER}-${ENVIRONMENT}" ~/.kube/config-${CLUSTER}
  export KUBECONFIG=~/.kube/config-${CLUSTER}
}

if [[ "$PURGE_DEPLOYMENT" == "delete" ]]; then
  kubeconfig_setup
  purge_deployment
  exit 0
fi

# function artifact_download()
# {
#   curl -sS -O -H "X-JFrog-Art-Api:${ARTIFACTORY_API_KEY}"  https://artifacts.apple.com/aml-kairos-release-local/com/apple/aml/kairos/infra/helm-charts/shuri-auth/7e83a2c1e341-2/shuri-auth-0.2.0.tgz
# }

function latest_artifact_download() {
  BUILD_NUMBER=$(curl -k -H "X-JFrog-Art-Api:${ARTIFACTORY_API_KEY}" $ARTIFACTORY_ROOT_URL/ | cut -d "/" -f 1 | grep href | cut -d "=" -f 2 | cut -d '-' -f 2 | sort -n | tail -1)
  echo $BUILD_NUMBER
  BUILD_ID=$(curl -k -H "X-JFrog-Art-Api:${ARTIFACTORY_API_KEY}" $ARTIFACTORY_ROOT_URL/ | cut -d "/" -f 1 | grep href | cut -d "=" -f 2 | cut -d '"' -f 2 | grep -e "-$BUILD_NUMBER")
  echo $BUILD_ID
  LATEST_CHART=$(curl -k -H "X-JFrog-Art-Api:${ARTIFACTORY_API_KEY}" $ARTIFACTORY_ROOT_URL/$BUILD_ID/ | grep "tgz" | cut -d '"' -f 2)
  echo $LATEST_CHART
  curl -k -sS -O -H "X-JFrog-Art-Api:${ARTIFACTORY_API_KEY}" $ARTIFACTORY_ROOT_URL/$BUILD_ID/$LATEST_CHART
  echo "$LATEST_CHART : downloaded."
  if [ ! -f $LATEST_CHART ]; then
    if [ ! $? -eq 0 ]; then
      echo "There was some problem during downloading latest chart"
      exit 0
    fi
  fi
}

function clear_old_resources() {
  if [[ "${CLUSTER}" == "us-west-1a" || "${CLUSTER}" == "us-east-1a" || "${CLUSTER}" == "us-west-2a" || "${CLUSTER}" == "us-west-3a" ]]; then
    kubectl delete rs $(kubectl get rs | awk -v appname="${BUILD_PARAM_Chart_Name}" '$1 ~ appname &&  $2==0 && $3==0 && $4==0 {print $1}') -n ${NAMESPACE} || true
    maxrev=$(helm history -n ${NAMESPACE} ${BUILD_PARAM_Chart_Name} | awk '/superseded|deleted/{print $1}' | tail -1)
    for i in $(seq 1 $(($maxrev - 2))); do kubectl delete secret sh.helm.release.v1.${BUILD_PARAM_Chart_Name}.v$i -n ${NAMESPACE} || true; done
  else
    helm tiller run ${NAMESPACE} -- helm history ${BUILD_PARAM_Chart_Name} | awk '/SUPERSEDED|DELETED/{print $1}' | (
      maxrev=$(tail -1)
      for i in $(seq 1 $(($maxrev - 2))); do kubectl delete secret ${BUILD_PARAM_Chart_Name}.v$i -n ${NAMESPACE} || true; done
    ) || true
    kubectl delete rs $(kubectl get rs | awk -v appname="${BUILD_PARAM_Chart_Name}" '$1 ~ appname &&  $2==0 && $3==0 && $4==0 {print $1}') -n ${NAMESPACE} || true
  fi
}

function helm_install() {

  if [[ "${CLUSTER}" == "us-west-1a" || "${CLUSTER}" == "us-east-1a" || "${CLUSTER}" == "us-west-3a" ]]; then
    helm upgrade --history-max 2 --namespace "${NAMESPACE}" ${APPNAME} -f environments/values-${CLUSTER}-${NAMESPACE}.yaml --set image.tag=${BUILD_PARAM_DOCKER_IMAGE_TAG} --set app.applicationProperties.shuriVersion=${BUILD_PARAM_SHURI_VERSION} $LATEST_CHART --install --force --debug

  elif [[ "${CLUSTER}" == "us-west-2a" ]]; then
    helm upgrade --history-max 2 --namespace "${NAMESPACE}" ${APPNAME} -f environments/values-${CLUSTER}-${NAMESPACE}.yaml --set image.tag=${GIT_COMMIT_SHORT} --set app.applicationProperties.shuriVersion="$(date '+%m.%d.%Y'.01-DEV)" $LATEST_CHART --install --force --debug

  elif [[ "${NAMESPACE}" == "aml-shuri-dev" ]]; then
    clear_old_resources
    helm tiller run "${NAMESPACE}" -- helm upgrade ${APPNAME} -f environments/values-${CLUSTER}-${NAMESPACE}.yaml --set image.tag=${GIT_COMMIT_SHORT} --set app.applicationProperties.shuriVersion="$(date '+%m.%d.%Y'.01-DEV)" $LATEST_CHART --install --force --debug
  else
    clear_old_resources
    helm tiller run "${NAMESPACE}" -- helm upgrade ${APPNAME} -f environments/values-${CLUSTER}-${NAMESPACE}.yaml --set image.tag=${BUILD_PARAM_DOCKER_IMAGE_TAG} --set app.applicationProperties.shuriVersion=${BUILD_PARAM_SHURI_VERSION} $LATEST_CHART --install --force --debug
  fi
}

kubeconfig_setup
latest_artifact_download
#artifact_download
helm_install
