# Pixels Daemon

In the Pixels cluster, there is one `Coordinator` process and one or more `Worker` processes.
The `Coordinator` is generally deployed on the same node of the query engine's master
(or coordinator).
While the `Workers` are deployed together with the query engine workers and storage data nodes.

Either `Coordinator` or `Worker` process is started as a **stateless** daemon process in the server.
Whenever the daemon process is crashed or killed, it only needs a restart to recover.
