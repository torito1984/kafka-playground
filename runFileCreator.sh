#/usr/bin/bash

mkdir -p ${1}
java -cp target/kafka-message-example-0.0.1.jar  com.kafka.message.server.example.other.launch.CreateFile --path ${1} --sleepTime ${2}