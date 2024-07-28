# Kafka Framework
This project enhances Kafka clients' functionality. 

## Project overview
- **Centralized Kafka clients configuration**: 
It provides a service that automatically updated Kafka clients' configuration.<br>
Sequence diagram<br>
![](images/sequence/AutoReconfiguration.svg)
- **Kafka based cache**: A key-value cache that stores data in Kafka.<br>
Sequence diagram<br>
![](images/sequence/kafkaCache.svg)
- **Kafka consumer concurrent processing**: A mechanism to process records in parallel.<br> 
Kafka built-in consumer group<br>
![](images/other/consumer_group.svg)
Alternative approach<br>
![](images/other/threads_pool.svg)
- **Robust Kafka producer**: A fault-tolerant producer with non-blocking sending.
It uses a local storage in order not to lose records.<br>
Sequence diagram<br>
![](images/sequence/producer.svg)

## Implementation details
- **Centralized Kafka clients configuration**<br>
Components diagram<br>
![](images/components/configServiceComponents.svg)<br>
Class diagram<br>
![](images/classes/ConfigurationClasses.svg)
- **Kafka based cache**<br>
Class diagram<br>
![](images/classes/cacheDiagram.svg)
- **Kafka consumer concurrent processing**<br>
Components diagram<br>
![](images/components/consumerComponents.svg)<br>
Class diagram<br>
![](images/classes/ConsumerClasses.svg)
- **Robust Kafka producer**:<br>
Components diagram<br>
![](images/components/producerComponents.svg)<br>
Class diagram<br>
![](images/classes/producerDiagram.svg)
