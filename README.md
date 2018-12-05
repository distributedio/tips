# Tips 

基于TiKV的分布式Pub/Sub系统

[![Go Report Card](https://goreportcard.com/badge/github.com/shafreeck/tips)](https://goreportcard.com/report/github.com/shafreeck/tips)
[![Build Status](https://travis-ci.org/shafreeck/tips.svg?branch=master)](https://travis-ci.org/shafreeck/tips)
[![Coverage Status](https://coveralls.io/repos/github/shafreeck/tips/badge.svg?branch=master)](https://coveralls.io/github/shafreeck/tips?branch=master)
[![GoDoc](https://godoc.org/github.com/tipsio/tips?status.svg)](https://godoc.org/github.com/shafreeck/tips)

## 功能特性

* 高性能、高可用、水平扩展
* 支持海量Topic个数，以及单Topic海量数据
* Topic全局有序
* At-Least-Once的可靠通信
* 多消费者并发处理（类似Kafka Consumer Group）
* 支持订阅状态的快照和恢复

## 应用场景

* 异步任务处理（比如图片，视频等）
* 事件驱动模型架构（比如微服务，Severless等）
* 多对多消息通信（比如即时聊天，大规模直播间群聊等）
* 移动设备消息推送

