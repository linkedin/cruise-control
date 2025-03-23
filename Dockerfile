FROM openjdk:21-jdk-slim

COPY . .

RUN chmod +x kafka-cruise-control-start.sh

RUN ./gradlew clean jar copyDependantLibs --warning-mode all

EXPOSE 5600

CMD ["./kafka-cruise-control-start.sh", "config/cruisecontrol.properties", "5600"]
