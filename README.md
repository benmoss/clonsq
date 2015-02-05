# clonsq

An in-progress Clojure client library for [NSQ](http://nsq.io/).

## Usage

Examples are available in [the examples directory](/examples) and
can be run via Leiningen (`lein run -m nsq-tail`).

## TODOS

- Publishing
- Multiple nsqlookups
- Direct connections
- Polling nsqlookups
- IDENTIFY command
- Backoff
- REQ / TOUCH
- Jitter
- TLS
- Sampling
- AUTH
- Snappy compression
- Handle `max_in_flight < num_conns` scenario

## License

Copyright © 2015 Ben Moss

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
