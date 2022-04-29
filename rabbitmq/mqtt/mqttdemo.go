package main

import (
	//"bufio"
	"fmt"
	"log"
	"flag"
	"net/url"
	//"os"
	"strings"
	"time"

	"github.com/tjarratt/babble"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var QOS_AT_MOST_ONCE = byte(0)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func connect(uri *url.URL) mqtt.Client {
	password, _ := uri.User.Password()
	name := uri.User.Username()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", uri.Host))
	opts.SetUsername(name)
	opts.SetPassword(password)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(time.Microsecond) {
	}
	failOnError(token.Error(), "Failed while connecting")
	return client
}

func showMessage(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("* %s\n", string(msg.Payload()))
}

func parseTopic(uri *url.URL) string {
	topic := uri.Path[1:]
	if topic == "" {
		topic = "default"
	}
	return topic
}

func main() {
	user := flag.String("u", "user", "user name")
	password := flag.String("p", "password", "password")
	flag.Parse()
	mqttURL := fmt.Sprintf("mqtt://%s:%s@localhost:1883/test", *user, *password)
	uri, err := url.Parse(mqttURL)
	failOnError(err, "Failed to parse given URL")

	go func(uri *url.URL) {
		client := connect(uri)
		client.Subscribe(parseTopic(uri), QOS_AT_MOST_ONCE, showMessage)
	}(uri)

	client := connect(uri)
	RETAIN_MESSAGE := false

	babbler := babble.NewBabbler()
	babbler.Separator = " "

	for {
		//r := bufio.NewReader(os.Stdin)
		//msg, _ := r.ReadString('\n')
		msg := fmt.Sprintf("%s: %s", *user, strings.TrimSpace(babbler.Babble()))
		client.Publish(parseTopic(uri), QOS_AT_MOST_ONCE, RETAIN_MESSAGE, msg)
		time.Sleep(time.Second * 1)
	}
}

