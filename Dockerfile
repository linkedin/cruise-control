FROM --platform=linux/amd64 eclipse-temurin:11

USER root
RUN apt update
RUN apt install -y wget tar zip jq
WORKDIR /app

# Copy JARs
COPY ./cruise-control/build/libs/cruise-control-*.jar ./cruise-control/cruise-control/build/libs/cruise-control.jar
COPY ./cruise-control/build/dependant-libs ./cruise-control/cruise-control/build/dependant-libs
COPY ./kafka-cruise-control-start.sh ./cruise-control
COPY ./scripts/docker/start.sh ./

# Copy Config Files
COPY ./config ./cruise-control/config

# Replace values on cruisecontrol.properties
WORKDIR /app/cruise-control/config
RUN sed -i "s/com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler/com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.PrometheusMetricSampler/g" cruisecontrol.properties && \
    sed -i "s/webserver.http.port=9090/webserver.http.port=9091/g" cruisecontrol.properties && \
    sed -i "s/capacity.config.file=config\/capacityJBOD.json/capacity.config.file=config\/capacityCores.json/g" cruisecontrol.properties && \
    sed -i "$ a\kafka.broker.failure.detection.enable=true" cruisecontrol.properties && \
    sed -i '/^zookeeper\.connect/d' cruisecontrol.properties && \
    sed -i "s|webserver.http.cors.enabled=false|webserver.http.cors.enabled=true|g" cruisecontrol.properties && \
    sed -i "s|webserver.http.cors.origin=http://localhost:8080|http://localhost:9091|g" cruisecontrol.properties && \
    sed -i "s|webserver.http.cors.exposeheaders=User-Task-ID|webserver.http.cors.exposeheaders=User-Task-ID,Content-Type|g" cruisecontrol.properties && \
    echo "prometheus.server.endpoint=localhost:9090" >> cruisecontrol.properties && \
    echo "topics.excluded.from.partition.movement=__consumer_offsets.*|__amazon_msk_canary.*|__amazon_msk_connect.*" >> cruisecontrol.properties && \
    echo "broker.capacity.config.resolver.class=com.linkedin.kafka.cruisecontrol.config.AmazonMSKBrokerCapacityConfigResolver" >> cruisecontrol.properties

WORKDIR /app
# Download Prometheus
RUN mkdir -p prometheus && \
    wget -O prometheus.tar.gz https://github.com/prometheus/prometheus/releases/download/v2.51.1/prometheus-2.51.1.linux-amd64.tar.gz && \
    tar -xzvf prometheus.tar.gz -C prometheus --strip-components=1 && \
    rm prometheus.tar.gz

COPY ./prometheus-config/prometheus.yml ./prometheus/

WORKDIR /app/cruise-control
# Download Cruise Control UI
RUN wget -O cruise-control-ui.tar.gz https://github.com/linkedin/cruise-control-ui/releases/download/v0.3.4/cruise-control-ui-0.3.4.tar.gz && \
    tar -xvzf cruise-control-ui.tar.gz && \
    rm cruise-control-ui.tar.gz

# Download AWS CLI
WORKDIR /tmp
RUN wget -O awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip && \
    unzip awscliv2.zip && \
    sh /tmp/aws/install && \
    aws --version && \
    rm -rf /tmp/*

RUN chmod 777 -R /app

EXPOSE 9091

CMD [ "/app/start.sh" ]