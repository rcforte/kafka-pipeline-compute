### Pending
* Create a spark application that streams from kafka
* Calculate risk stats in kafka
* Publish risk stats to output topic
* Read the message (with correlation id) from spring boot

### DONE

* Install kafka
* Setup a topic an input and an output topic
* Create a spring boot app that publishes to input topic
* Create a react application that shows a chart with the price
 
 
### DOCS
 
#### Create a new react application:

`npx create-react-app <ApplicationName>`

#### Install react chart library:
`npm install recharts`

####Start Zookeeper
   
 `bin/zookeeper-server-start.sh config/zookeeper.properties`
 
 `bin/kafka-server-start.sh config/server.properties`
 
 #### Create a Topic
 `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor    1 --partitions 1 --topic PricesInput`
 
 `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic PricesInput`
 
 `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic         PricesInput`
 
 
