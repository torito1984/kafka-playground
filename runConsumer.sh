#/usr/bin/bash

java -cp target/kafka-message-example-0.0.1.jar com.kafka.message.server.example.launch.MailConsumerDemo --topic ${1} --offset ${2} --group ${3}
