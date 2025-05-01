## Build a tcp-based simple Chat app 

Step 1: Set Up a TCP Server
Listen on a port

Accept incoming connections

Step 2: Handle Each Client in a Goroutine
For every new connection, spawn a new goroutine

Read messages from the client and send to a shared channel

Step 3: Broadcast Messages to All Clients
Maintain a list of active clients (maybe with a map)

Create a broadcaster goroutine that reads from the shared channel and sends to all clients

Step 4: Add Graceful Client Disconnect Handling
Remove disconnected clients

Avoid broken pipes