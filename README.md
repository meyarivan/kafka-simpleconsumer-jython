bagheera-kafka-simpleconsumer-jython
====================================

Simple consumer for Bagheera messages stored in Kafka which writes data to stdout and offsets to stderr.

Requires following libraries under lib/ subdir.

* jopt-simple-3.2.jar
* kafka-0.7.1.jar
* log4j-1.2.15.jar
* protobuf-java-2.4.1.jar
* scala-library.jar
* snappy-java-1.0.4.1.jar
* zkclient-0.1.jar
* zookeeper-3.3.4.jar
* bagheera-0.15.jar
* fastjson-1.1.39.jar

To run / test the consumer:

* Update config.py with relevant values for your environment
* Generate initial offsets using any existing data or use gen_offsets.py
* Run :)

```
# replace MAXHEAP with appropriate heapsize (ex: no of brokers * no of partitions * 64MB * 1.5)
java -XmxMAXHEAP -cp jython-standalone-2.7-b1.jar org.python.util.jython main.py file_containing_offsets

```

Data is written to stdout as space separated values consisting of request_type, timestamp_millis, ip_addr and payload

Offset data is periodically written to stderr as dict serialized to json containing the keys partition, topic, metrics, offset, hostname and time_millis

