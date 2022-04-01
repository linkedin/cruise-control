## Run without ZooKeeper ##

Cruise Control can run without ZooKeeper. This can be useful for testing it with [Kafka in KRaft mode](https://github.com/apache/kafka/tree/trunk/config/kraft) 
or for running it in environments where ZooKeeper can't be accessed. 

In order to do so, users need to modify `config/cruisecontrol.properties` of Cruise Control:
* Set `kafka.broker.failure.detection.enable` to `true`
* Ensure `topic.config.provider.class` is set to `com.linkedin.kafka.cruisecontrol.config.KafkaAdminTopicConfigProvider`
* Remove `zookeeper.connect` and any ZooKeeper security settings (Optional)
