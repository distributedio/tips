# Tips 

A distributed Pub/Sub system based on TiKV

[![Go Report Card](https://goreportcard.com/badge/github.com/tipsio/tips)](https://goreportcard.com/report/github.com/tipsio/tips)
[![Build Status](https://travis-ci.org/tipsio/tips.svg?branch=master)](https://travis-ci.org/tipsio/tips)
[![Coverage Status](https://coveralls.io/repos/github/tipsio/tips/badge.svg?branch=master)](https://coveralls.io/github/tipsio/tips?branch=master)
[![GoDoc](https://godoc.org/github.com/tipsio/tips?status.svg)](https://godoc.org/github.com/tipsio/tips)

## Features

* High performance, high availability, horizontal scaling
* Massive Topics support and massive data support for single Topic
* Topic kept in a global order
* At-Least-Once reliable communication
* Support concurrent consumers (like, Kafka Consumer Group)
* Snapshot and recovery of subscription state

## Scenarios

* Asynchronous task processing (e.g. pictures/ videos)
* Event-driven framework (e.g. microservice/Severless )
* Multi-to-multi message communication (e.g. IM/ group chat in live-broadcasting room with large scale )
* Notifications push on mobile devices
