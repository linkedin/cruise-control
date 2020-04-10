FROM adoptopenjdk/openjdk11:jdk-11.0.6_10-slim
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates
RUN mkdir /cruise-control
COPY . /cruise-control
RUN cd cruise-control && echo && ./gradlew jar copyDependantLibs

RUN mv -v /cruise-control/cruise-control/build/libs/cruise-control-*.jar \
  /cruise-control/cruise-control/build/libs/cruise-control.jar
RUN mv -v /cruise-control/cruise-control/build/dependant-libs/cruise-control-metrics-reporter-*.jar \
  /cruise-control/cruise-control/build/dependant-libs/cruise-control-metrics-reporter.jar

FROM node:10.19
RUN mkdir /src && cd /src && git clone https://github.com/amuraru/cruise-control-ui.git
WORKDIR /src/cruise-control-ui
RUN git fetch origin
RUN git checkout master
RUN git pull
RUN git rev-parse HEAD
RUN npm install
RUN npm run build

FROM openjdk:14-slim
ARG SOURCE_REF
ARG SOURCE_TYPE
ARG DOCKERFILE_PATH
ARG VERSION

RUN mkdir -p /opt/cruise-control /opt/cruise-control/cruise-control-ui
COPY --from=0 /cruise-control/cruise-control/build/libs/cruise-control.jar /opt/cruise-control/cruise-control/build/libs/cruise-control.jar
COPY --from=0 /cruise-control/config /opt/cruise-control/config
COPY --from=0 /cruise-control/kafka-cruise-control-start.sh /opt/cruise-control/
COPY --from=0 /cruise-control/cruise-control/build/dependant-libs /opt/cruise-control/cruise-control/build/dependant-libs
COPY docker/opt/cruise-control /opt/cruise-control/
COPY --from=1 /src/cruise-control-ui/dist /opt/cruise-control/cruise-control-ui/dist
RUN echo "local,localhost,/kafkacruisecontrol" > /opt/cruise-control/cruise-control-ui/dist/static/config.csv

EXPOSE 8090
CMD [ "/opt/cruise-control/start.sh" ]
