# pixels-daemon

In the Pixels cluster, there is a process called `Coordinator`
while the other processes are called `DataNode`.
The `Coordinator` is generally deployed on the same node where the master
(or coordinator) of the query engine and storage systems (e.g., HDFS) are running.
The `DataNodes` are deployed together with the query engine worker and storage
data nodes.

Either `Coordinator` or `DataNode` process is started as a **stateless** daemon process in the server.
And each of them are protected by a `Guard` process. Whenever the daemon process is crashed or killed,
the guard process will restart it, and vice versa.
