# pixels-daemon

In the Pixels cluster, there is one `Coordinator` process and one or more `DataNode` processes.
The `Coordinator` is generally deployed on the same node of the query engine's master
(or coordinator).
While the `DataNodes` are deployed together with the query engine workers and storage
data nodes.

Either `Coordinator` or `DataNode` process is started as a **stateless** daemon process in the server.
And each of them is protected by a `Guard` process. Whenever the daemon process is crashed or killed,
the guard process will restart it, and vice versa.
