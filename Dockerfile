ARG DEFAULT_DOCKER_REPO

FROM gradle:jdk15 AS build
COPY --chown=gradle:gradle . /workspace
WORKDIR /workspace

RUN ./gradlew jar copyDependantLibs --info

FROM openjdk:15-slim
COPY --from=build /workspace/cruise-control/build/dependant-libs/ /app/cruise-control/build/dependant-libs/
COPY --from=build /workspace/cruise-control/build/libs/ /app/cruise-control/build/libs/
COPY --from=build /workspace/config/ /app/config/
COPY --from=build /workspace/*.sh /app/

RUN ls /app/cruise-control -lah

EXPOSE 9090

ENTRYPOINT ["/app/kafka-cruise-control-start.sh", "/app/config/cruisecontrol.properties"]
