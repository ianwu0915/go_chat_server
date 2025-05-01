package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	// create a map to maintain a list of active clients with mutex lock
	clients = make(map[net.Conn]Client)
	// Create a channel to stored all the recieved message to send to everyone in the chat with mutex lock
	messageChannel = make(chan Message, 1024) // concurrent-safe

	mutex = &sync.Mutex{}
)

type Client struct {
	Name     string
	Conn     net.Conn
	JoinedAt time.Time
}

type Message struct {
	Sender  Client
	Message string
}

func main() {

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Creating Server failed due to: ", err)
		return
	}
	defer listener.Close()

	go broadcaster()
	fmt.Println("Server is listening on port 8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error receiving connection: ", err)
			continue
		}

		// Hanlde Client Connection
		// let client send message to the shared channel and the server will broadcast to all other clients
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	// add the client into the pool

	client := Client{
		Name:     conn.RemoteAddr().String(),
		Conn:     conn,
		JoinedAt: time.Now(),
	}

	mutex.Lock()
	clients[conn] = client
	mutex.Unlock()

	defer removeClients(conn)

	//讀取client message、格式化、加上id/名稱
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() { // 出問題時，會結束回圈 （client關閉連線、server端有穩提、收到EOF
		line := scanner.Text()
		fmt.Println(line)
		message := Message{client, line}
		messageChannel <- message
	}
	//如果 client斷線 （讀不到訊息、EOF) 就移除這個client, 關閉連線
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	} else {
		fmt.Println("client disconnected")
	}
}

func broadcaster() {
	// Continue to recieve message from the channel
	// Send the message to all the clients except for the sender itseld
	for msg := range messageChannel { //blocking call
		sender := msg.Sender
		for conn := range clients {
			if conn != sender.Conn {
				_, err := conn.Write([]byte(msg.formatMessage() + "\n"))
				if err != nil {
					removeClients(conn)
				}
			}
		}
	}
}

func removeClients(client net.Conn) {
	mutex.Lock()
	delete(clients, client)
	client.Close()
	mutex.Unlock()
}

func (m Message) formatMessage() string {
	return fmt.Sprintf("[%s]: %s", m.Sender.Name, m.Message)
}
