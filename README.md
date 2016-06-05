Kafka Example Application
========================================

Kafka example application

This project shows the basics of Kafka queue system when leveraged by Java. The code simulates a producer that is reading a set of emails to send and a consumer that picks them so they are processed. The pieces of the code are:

- KafkaMailConsumer: Consumer that picks mails from the Kafka queue.
- KafkaMailProducer: Producer that picks mails from the filesystem and queues them in kafka.
- CreateFile: Simulates the creation of files on a folder so they are picked up by the producer.

In addition, and for practice purposes, IntegerSerializer and IntegerDeserializer depict how to write a custom message serializer and CustomPartitioner shows how to write a custom partitioner for sharding a topic.

This code supposes that a Kafka installation is available. Namely, it has been tested with Kafka 0.9.0.4 packaged in Hortonworks HDP 2.4.0.0.

Instructions to run
========================================

In order to run the code, 

- runFileCreator.sh triggers the mail creation simulation. Run one of this
- runProducer.sh runs the producer to Kafka. It takes as arguments the folder where the FileCreator is pushing mails and the topic where to publish the messages. Kafka installation is supposed to be listening in localhost:6667. This default can be changed with a simple change in the code.
- runConsumer.sh runs the mail consumer. You can run as many consumers as you want (note that if you run more consumers than partitions of the topic some of them will remain idle). It takes as argumens the topic, the point where to start consuming (e.g. earliest) and the name of the consumer group.



