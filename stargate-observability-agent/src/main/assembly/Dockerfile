FROM        docker.apple.com/shuri/oss-ignite:2.14.0
MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>
COPY        stargate-observability-agent/.out/distributions/stargate-observability-agent-*.zip /opt/
WORKDIR     /opt
RUN         unzip stargate-observability-agent-*.zip
RUN         rm stargate-observability-agent-*.zip
RUN         mv stargate-observability-agent-* stargate
EXPOSE      8080
ENV         APP_HOME /opt/stargate
ENV         APP_LOG_FILE_NAME stargate-observability-agent
CMD         ["/bin/sh", "stargate/bin/stargate-observability-agent"]