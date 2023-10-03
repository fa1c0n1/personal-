#!/bin/bash

export ENVIRONMENT=${BUILD_PARAM_ENVIRONMENT}
export user=$(cat ${BUILD_SECRETS_PATH}/user_${BUILD_PARAM_ENVIRONMENT})
export password=$(cat $BUILD_SECRETS_PATH/password_$BUILD_PARAM_ENVIRONMENT)
cp $BUILD_SECRETS_PATH/cert_$BUILD_PARAM_ENVIRONMENT client.pem

if [[ "${ENVIRONMENT}" == "DEV" ]]; then
  echo "SELECTED ENVIRONMENT IS DEV"
  echo "use stargate-dev;" >init.sql
  cat stargate-admin-service/src/main/sql/schema/*.sql >>init.sql
  export mongohost="mr51q01nt-maasdlmdb002.dbs.ise.apple.com"
  export mongoport='10905'

elif [[ "${ENVIRONMENT}" == "QA" ]]; then
  echo "SELECTED ENVIRONMENT IS QA"
  echo "use stargate-qa;" >init.sql
  cat stargate-admin-service/src/main/sql/schema/*.sql >>init.sql
  export mongohost="st52q01nt-maasdlmdb001.dbs.ise.apple.com"
  export mongoport='10906'

elif [[ "${ENVIRONMENT}" == "UAT" ]]; then
  echo "SELECTED ENVIRONMENT IS UAT"
  echo "use stargate-uat;" >init.sql
  cat stargate-admin-service/src/main/sql/schema/*.sql >>init.sql
  export mongohost="st52q01nt-maasdlmdb001.dbs.ise.apple.com"
  export mongoport='10911'

elif [[ "${ENVIRONMENT}" == "PROD" ]]; then
  echo "SELECTED ENVIRONMENT IS PROD"
  echo "use stargate;" >init.sql
  cat stargate-admin-service/src/main/sql/schema/*.sql >>init.sql
  export mongohost="ma4-mi2p-lmdb04.corp.apple.com"
  export mongoport='10909'

else
  echo "ENVIRONMENT IS NOT CORRECT"
  exit 0
fi

mongo --tls --tlsCertificateKeyFilePassword "${password}" --tlsCertificateKeyFile 'client.pem' --tlsCAFile "/etc/ssl/certs/ca-certificates.crt" --authenticationDatabase '$external' --authenticationMechanism MONGODB-X509 -u "${user}" --host $mongohost --port ${mongoport} <init.sql

if [[ $? -eq 0 ]]; then
  echo "Schema migrated successfully"
  rm init.sql
else
  echo "Schema migration failed. Please check the logs."
fi
