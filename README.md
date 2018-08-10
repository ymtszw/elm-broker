# elm-broker

[![CircleCI](https://circleci.com/gh/ymtszw/elm-broker/tree/master.svg?style=svg)](https://circleci.com/gh/ymtszw/elm-broker/tree/master)

Data stream buffer for Elm application, inspired by [Apache Kafka](https://kafka.apache.org/).

## What is this?

- `Broker` is essentially a [circular buffer](https://www.wikiwand.com/en/Circular_buffer), internally using `Array`
- Read pointers (`Offset`) are exposed to clients, allowing "pull"-style data consumption, just as in Kafka
- Insert(`append`), `read`, and `update` all take _O(1)_
- A buffer is made of multiple `Segment`s. Buffer size (= number of `Segment`s and size of each `Segment`) can be configured
- When the whole buffer is filled up, a new "cycle" begins and old `Segment`s are evicted one by one

## Expected Usage

- A `Broker` accepts incoming data stream. <sup>&dagger;</sup>
- Several **consumers** reads ("pulls") data from the `Broker` individually, while maintaining each `Offset` as their internal states.
- Consumers perform arbitrary operations against acquired data, then read `Broker` again after previous `Offset`. Rinse and repeat.

<small><small>

&dagger;
It is possible to have multiple `Broker`s in your application for different purposes,
however you must be careful so that you do not mix up `Offset`s produced from one `Broker` to ones from others.
Since `Offset`s are only valid for their generating `Broker`.
Wrapping `Offset`s in phantom types is a possible technique to enforce this restriction.

</small></small>

## Remarks

- Technically, it can also perform _O(1)_ `delete`, but it is still unclear whether we want `delete` API
    - Original Kafka now [supports this as an admin command](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/admin/DeleteRecordsCommand.scala)
- There are several major features I am interested in:
    - Dump/serialize data into blob, such that it can be used to re-initialize a Broker at application startup
    - Bulk append and bulk read
    - Callback mechanism around `Segment` eviction
- Although bulk read is possible feature to add, do note that consumers may perform **arbitrary** operation against data,
  meaning that some consumers may require longer time than others.
    - If bulk reads (thus chunked consumer operations) are allowed, it could lead to the situation
      where some consumers "lock" entire application until their long-running operations are done.
    - In that sense one-by-one read (or smaller bulk read) could promote better interleaving of consumer operations.
    - Anyway these considerations are just off-my-head things that may be flipped or scrapped at any moment.

## Development

Install Elm Platform.

```sh
$ elm-package install
$ elm-test                    # full test
$ elm-test tests/MainTest.elm # only light-weight tests
```

## License

&copy; Copyright 2018 [Yu Matsuzawa](https://github.com/ymtszw)

BSD-3-Clause
