FROM        docker.apple.com/base-images/ubi9/java11-builder
MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>
COPY        stargate-kubernetes-operator/.out/distributions/stargate-kubernetes-operator-*.zip /opt/
WORKDIR     /opt

RUN          unzip stargate-kubernetes-operator-*.zip && \
                    rm stargate-kubernetes-operator-*.zip && \
                    mv stargate-kubernetes-operator-* stargate

EXPOSE      8080 5005 4000
ENV         APP_HOME=/opt/stargate \
                  APP_MODE=PROD \
                  APP_LOG_FILE_NAME=stargate-kubernetes-operator

CMD         ["/bin/sh", "stargate/bin/stargate-kubernetes-operator"]