package main

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/akamensky/argparse"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var QOS_AT_MOST_ONCE = byte(0)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func parseUserArgs() (string, string) {
	parser := argparse.NewParser("Chat", "Chat using AMQP and Go")

	// Create flags for username and password
	nameOptions := argparse.Options{Required: true, Help: "User name"}
	name := parser.String("u", "user", &nameOptions)
	passwordOptions := argparse.Options{Required: true, Help: "Password"}
	password := parser.String("p", "password", &passwordOptions)

	// Parse input
	err := parser.Parse(os.Args)
	failOnError(err, "Error while parsing arguments")
	return *name, *password
}

func connect(uri *url.URL) mqtt.Client {
	opts := createClientOptions(uri)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(time.Microsecond) {
	}
	failOnError(token.Error(), "Failed while connecting")
	return client
}

func createClientOptions(uri *url.URL) *mqtt.ClientOptions {
	password, _ := uri.User.Password()
	name := uri.User.Username()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", uri.Host))
	opts.SetUsername(name)
	opts.SetPassword(password)
	return opts
}

func showMessage(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("* %s\n", string(msg.Payload()))
}

func listen(uri *url.URL) {
	client := connect(uri)
	client.Subscribe(parseTopic(uri), QOS_AT_MOST_ONCE, showMessage)
}

func sendMessage(msg string, uri *url.URL) {
	client := connect(uri)
	RETAIN_MESSAGE := false
	client.Publish(parseTopic(uri), QOS_AT_MOST_ONCE, RETAIN_MESSAGE, msg)
}

func poolMessage(uri *url.URL, user string) {
	for {
		r := bufio.NewReader(os.Stdin)
		msg, _ := r.ReadString('\n')
		msg = fmt.Sprintf("%s: %s", user, strings.TrimSpace(msg))
		sendMessage(msg, uri)
	}
}

func parseTopic(uri *url.URL) string {
	topic := uri.Path[1:]
	if topic == "" {
		topic = "default"
	}
	return topic
}

func main() {
	user, passwd := parseUserArgs()
	fullUrl := fmt.Sprintf("mqtt://%s:%s@localhost:1883/test", user, passwd)
	uri, err := url.Parse(fullUrl)
	failOnError(err, "Failed to parse given URL")

	forever := make(chan bool)
	go listen(uri)
	go poolMessage(uri, user)
	<-forever
}

