# elm-broker

[![CircleCI](https://circleci.com/gh/ymtszw/elm-broker/tree/master.svg?style=svg)](https://circleci.com/gh/ymtszw/elm-broker/tree/master)

[Apache Kafka](https://kafka.apache.org/)-inspired timeseries data container.

## What is this?

- Essentially a [circular buffer](https://www.wikiwand.com/en/Circular_buffer), internally using `Array`
- Read pointers (`Offset`) are exposed to clients, allowing "pull"-style data consumption, just as in Kafka
- Insert(`append`), `read`, and `update` all take ![O(1)]
- A buffer is made of multiple `Segment`s. Buffer size (= number of `Segment`s and size of each `Segment`) can be configured
- When a whole buffer is filled up, a new "cycle" begins and old `Segment`s are evicted one by one

[O(1)]: https://latex.codecogs.com/png.latex?%5Cdpi%7B100%7D%20O%281%29

## Remarks

- Technically, it can also perform ![O(1)] `delete`, but it is still unclear whether we should introduce `delete` API
- There are several major features I am interested in:
    - Dump/serialize data into blob, such that it can be used to re-initialize a Broker at application startup
    - Callback mechanism around `Segment` eviction

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
