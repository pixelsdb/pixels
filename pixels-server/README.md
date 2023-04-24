# Pixels Server

`pixels-server` provides the REST and RPC APIs for external clients and modules to access the services in Pixels.
All the internal components in Pixels are deployed in an isolated network for security, and only the authorized 
and authenticated services are exposed to the external users.

## Usage

After building Pixels with maven, find `pixels-server-*.jar` in `pixels-server/target`.
Run it using the command:
```bash
java -jar pixels-server-*.jar
```
The default REST service port is 18890, and the default RPC service port is 18892.
A simple web UI is also provided on http://localhost:18890.

Optionally, you can use an external configuration file (i.e., `application.properties`) like this:
```bash
java -jar --spring.config.location=file:///[path-to-application.properties] pixels-server-*.jar
```
In the external `application.properties`, you can customize the configuration properties such  
as the server addresses and ports.