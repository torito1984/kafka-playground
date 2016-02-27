#/usr/bin/bash

java -cp target/kafka-message-example-0.0.1.jar com.kafka.message.server.example.launch.MailConsumerProducerDemo --path ${1} --topic ${2}