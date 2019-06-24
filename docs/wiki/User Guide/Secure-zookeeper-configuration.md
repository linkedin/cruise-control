#Secure zookeeper configuration

Cruise Control uses zookeeper clients for its operation. If the zookeeper is secured, the following
steps are to be taken care of so that the zookeeper client authenticates successfully.

* Set the config _zookeeper.security.enabled_ in "$base_dir/config/cruisecontrol.properties" to _true_.
* Rename the file "$base_dir/config/cruise_control_jaas.conf_template" to "$base_dir/config/cruise_control_jaas.conf". 
* In the file cruise_control_jaas.conf, enter the appropriate _Client{ .. }_ entry for the zookeeper client.

Cruise Control will export the "$base_dir/config/cruise_control_jaas.conf" configuration file only if it is present.
Please ensure that the jaas file contains the correct entry for successful authentication. The authentication failure/success
messages will appear in the Cruise Control logs on startup.

NOTE: If using the SASL protocol, you could enter the _KafkaClient{ .. }_ entry here as this configuration file will be
exported.