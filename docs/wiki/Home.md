# Welcome to Cruise Control

Cruise Control is a project that helps operate Kafka hands-free!

## Features
Cruise Control provides the following features out of the box:
* Continually balance a Kafka cluster with respect to disk, network and CPU utilization. 
* When a broker fails, automatically reassign replicas that were on that broker to other brokers in the cluster and restore the original replication factor. 
* Identify the topic-partitions that consume the most resources in the cluster. 
* Support one-click cluster expansions and broker decommissions. 
* Support heterogeneous Kafka clusters and multiple brokers per machine.

## Getting started
Follow our [quick start](https://github.com/linkedin/cruise-control#quick-start) and try it out.
You may also want to take a look at [configurations](https://github.com/linkedin/cruise-control/wiki/Configurations) to adjust the configurations by yourself.

## Reporting bugs
If you will encounter any issues please open an issue [here](https://github.com/linkedin/cruise-control/issues) and we encourage you to provide a patch through github pull request as well. 

## Contact
Join our [gitter room](https://gitter.im/kafka-cruise-control/Lobby) to interact with the community!
