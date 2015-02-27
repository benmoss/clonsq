# clonsq [![Build Status](https://travis-ci.org/benmoss/clonsq.svg?branch=master)](https://travis-ci.org/benmoss/clonsq)

An in-progress Clojure client library for [NSQ](http://nsq.io/).


## Usage

Examples are available in [the examples directory](/examples) and
can be run via Leiningen (`lein run -m nsq-tail`).

## TODOS

- Publishing
- Clean close (https://github.com/bitly/go-nsq/blob/596f98305c8f912a20bd54ee069ca13c8d84a012/conn.go#L593-L617)
- Direct connections
- IDENTIFY command
- Backoff
- REQ / TOUCH
- Jitter
- TLS
- Sampling
- AUTH
- Snappy compression
- Handle `max_in_flight < num_conns` scenario
- Handle consumer connection timeouts

## License

Copyright © 2015 Ben Moss

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
