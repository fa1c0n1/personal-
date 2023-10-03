FROM        docker.apple.com/shuri/oss-ignite:2.14.0
MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>
COPY        stargate-admin-service/.out/distributions/stargate-admin-service-*.zip /opt/
WORKDIR     /opt
RUN         unzip stargate-admin-service-*.zip
RUN         rm stargate-admin-service-*.zip
RUN         mv stargate-admin-service-* stargate
EXPOSE      8080
EXPOSE      8081
EXPOSE      8082
EXPOSE      8083
ENV         APP_HOME /op/stargate
ENV         APP_LOG_FILE_NAME stargate-service
RUN         mkdir /app  && \
                   ln -s /opt/stargate /app/stargate && \
                   ln -s /opt/stargate/bin/stargate-admin-service stargate/bin/stargate-service
CMD         ["/bin/sh", "stargate/bin/stargate-admin-service"]