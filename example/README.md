# Localhost cluster with 3 'nodes'

Before starting you should have rust toolchain installed on your machine.
Machine should run linux. Not tested on other OS's.
Git clone or download this project.

## Playground preparation

1. Open 4 terminal tabs or terminal windows.

2. Navigate three of them into parent sledributed directory (git cloned or downloaded dir)

3. On those three terminal windows execute following commands:
- First terminal: ```cargo run example/data_1```
- Second terminal: ```cargo run example/data_2```
- Third terminal: ```cargo run example/data_3```

All three windows should stop flooding retry and other debug messages and stay still.
Also example/data_1,data_2,data_3 dirs should have new subdirectories for sled database and replicated changelog.

## Kicking the tires

If you didn't change anything from the example/data_{1,2,3}/node.toml files you have three nodes running on localhost ports 8080, 8081 and 8082.

_Default logging level is DEBUG and after each command at least the targeted node should print debugging info_

On the fourth terminal window start running curl commands:
#### Try to get key value pair from the first node
```curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8080/key/eat```
expected failure:```{"Single":{"key":"eat","exists":false}}```

#### Insert key value pair to the second node
```curl -X POST -H "Content-Type: application/json" -d '{"key":"eat","value":"it"}' http://127.0.0.1:8081/key```
expected success:```{"Single":{"key":"eat","value":"it"}}```

#### Get inserted key value pair from the third node
```curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8082/key/eat```
expected success:```{"Single":{"key":"eat","exists":true,"value":"it"}}```

Now you could kill off one of the nodes to see if your cluster remains on the road without it.
Open terminal window for node 2 (listening on address 127.0.0.1:8081) and hit ```ctrl + c```

#### Get key value pair through the first node
Getting keys from the remaining nodes should work even if we're down to one node.

```curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8080/key/eat```
expected success:```{"Single":{"key":"eat","exists":true,"value":"it"}}```

#### Insert another key value pair to the third node
Inserting keys should work if we have 2 out of 3 nodes online and connected.

```curl -X POST -H "Content-Type: application/json" -d '{"key":"duck","value":"you"}' http://127.0.0.1:8082/key```
expected success:```{"Single":{"key":"duck","value":"you"}}```

If you bring up your node 2 again we can test if it picks up the changes after reconnecting to its peers.

Go back to second terminal window and run ```cargo run example/data_2```
You should see a message of snapshotted key value pairs being inserted.

#### Get the recently inserted key from through second node
This node was offline when key was inserted to third node.

```curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8081/key/duck```
expected success:```{"Single":{"key":"duck","exists":true,"value":"you"}}```

Kill 2 out of three nodes and see what happens when 'quorum' is lost.
Open terminal window for node 2 (listening on address 127.0.0.1:8081) and hit ```ctrl + c```
Open terminal window for node 3 (listening on address 127.0.0.1:8082) and hit ```ctrl + c```

#### Try to insert another key to the first node
Inserting keys should not work if we have 1 out of 3 nodes online.

```curl -X POST -H "Content-Type: application/json" -d '{"key":"in_your","value":"maze"}' http://127.0.0.1:8080/key```
expected failure:```{"Single":{"key":"in_your","error":"Action was blocked as too many cluster nodes have disconnected. Operation is permitted once more than half of the cluster nodes have returned"}}```

#### Try to get key and value from the first node
```curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8080/key/eat```
expected success:```{"Single":{"key":"eat","exists":true,"value":"it"}}```

Connect nodes 2 and 3 again to try deletes and bulk actions.
Go back to second terminal window and run ```cargo run example/data_2```
Go back to third terminal window and run ```cargo run example/data_3```

#### Insert multiple key value pairs to the first node
```curl -X POST -H "Content-Type: application/json" -d '[{"key":"goto","value":"dell"},{"key":"up","value":"doors"}]' http://127.0.0.1:8080/keys```
expected success:```{"Multiple":[{"key":"goto","value":"dell"},{"key":"up","value":"doors"}]}```

#### Get multiple keys from second node
```curl -X GET -H "Content-Type: application/json" -d '["goto","up"]' http://127.0.0.1:8081/keys```
expected success:```{"Multiple":[{"key":"goto","exists":true,"value":"dell"},{"key":"up","exists":true,"value":"doors"}]}```

#### Delete one key value pair from first node
```curl -X DELETE  http://127.0.0.1:8082/key/in_your```
expected success:```{"Single":{"key":"in_your","exists":true,"overwritten_value":"maze"}}```

#### Delete multiple keys from the third node
```curl -X DELETE -H "Content-Type: application/json" -d '["eat","goto"]' http://127.0.0.1:8082/keys```
expected success:```{"Multiple":[{"key":"eat","exists":true,"overwritten_value":"it"},{"key":"goto","exists":true,"overwritten_value":"dell"}]}```


### Ping node peers just for shits and giggles
Run the same command for ports 8081 and 8082 to see how their peers pong.

```curl -X GET  http://127.0.0.1:8080/ping```
expected success:```{"Multiple":[{"key":"3","value":"Pinged peer node 3 successfully"},{"key":"2","value":"Pinged peer node 2 successfully"}]}```

### Run any command through client websocket
First get a [websocat](https://github.com/vi/websocat) tool to play with or build your own client in javascript and run it from your browser or something.

If you choose to play with websocat connect to node 1:
```websocat ws://127.0.0.1:8080/client```

Insert command to send as json string and hit enter:
```{"APIRequest":{"Get":"duck"}}```

Response comes below the command when server is done.
expected success:```{"request":{"Get":"duck"},"response":{"Get":{"key":"duck","value":"you"}}}```