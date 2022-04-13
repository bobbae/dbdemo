## Basics / AMPQ using Rabbitmq

Run ./run-rabbitmq.sh

cd consumer; go run main.go

cd producer; go run main.go

## MQTT

https://lucastamoios.com/blog/2019/06/02/building-a-chat-from-scratch-with-go-and-mqtt/

Enabling and adding users to rabbitmq inside the docker container.

And docker exec -it rabbitmq bash

Inside the docker shell run

```
rabbitmq-plugins enable rabbitmq_mqtt
rabbitmqctl add_user user1 user1
rabbitmqctl add_user user2 user2
rabbitmqctl set_permissions -p / user1 ".*" ".*" ".*"
rabbitmqctl set_permissions -p / user2 ".*" ".*" ".*"

```

Run demo program 
```
$ cd mqtt; go run main.go -u user1 -p user1
hello user 1
* user1: hello user 1
goodbye now
* user1: goodbye now
* user1: 
^Csignal: interrupt
```
