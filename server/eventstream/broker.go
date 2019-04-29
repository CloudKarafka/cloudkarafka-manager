package eventstream

import (
	"fmt"

	"net/http"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

// the amount of time to wait when pushing a message to
// a slow client or a client that closed after `range clients` started.
const patience time.Duration = time.Second * 1

type Client struct {
	Channel      chan []byte
	Subscription string
}

type Broker struct {
	Name string
	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan []byte
	// New client connections
	newClients chan *Client
	// Closed client connections
	closingClients chan *Client
	// Client connections registry
	Clients map[*Client]bool
}

func NewBroker(name string) (broker *Broker) {
	// Instantiate a broker
	broker = &Broker{
		Name:           name,
		Notifier:       make(chan []byte, 1),
		newClients:     make(chan *Client),
		closingClients: make(chan *Client),
		Clients:        make(map[*Client]bool),
	}
	// Set it running - listening and broadcasting events
	go broker.listen()
	return
}

func (broker *Broker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	// Each connection registers its own message channel with the Broker's connections registry
	client := &Client{
		Channel:      make(chan []byte),
		Subscription: string
	}
	// Signal the broker that we have a new connection
	broker.newClients <- client
	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		broker.closingClients <- client
	}()
	// Listen to connection close and un-register messageChan
	notify := rw.(http.CloseNotifier).CloseNotify()
	for {
		select {
		case <-notify:
			return
		default:
			fmt.Fprintf(rw, "data: %s\n\n", <-client.Channel)
			// Flush the data immediatly instead of buffering it for later.
			flusher.Flush()
		}
	}
}

func (broker *Broker) listen() {
	for {
		select {
		case s := <-broker.newClients:
			broker.Clients[s] = true
			log.Info(fmt.Sprintf("broker_%s.add", broker.Name), log.MapEntry{"client": len(broker.clients)})
		case s := <-broker.closingClients:
			delete(broker.Clients, s)
			log.Info(fmt.Sprintf("broker_%s.remove", broker.Name), log.MapEntry{"client": len(broker.clients)})
		case event := <-broker.Notifier:
			for client, _ := range broker.Clients {
				select {
				case client.Channel <- event:
				case <-time.After(patience):
				}
			}
		}
	}

}
