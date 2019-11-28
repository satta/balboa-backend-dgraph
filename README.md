# balboa-backend-dgraph

This experimental backend implements a [balboa](https://github.com/DCSO/balboa) storage/query backend on top of the [Dgraph graph database](https://dgraph.io).

By default, it listens on all interfaces (`0.0.0.0`) on port 4242.
Use `SIGTERM` or `SIGINT` to terminate the server.

## Limitations

This is a proof of concept, so no documentation or performance measurements are available.