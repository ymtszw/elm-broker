# elm-broker

[![CircleCI](https://circleci.com/gh/ymtszw/elm-broker/tree/master.svg?style=svg)](https://circleci.com/gh/ymtszw/elm-broker/tree/master)

[Apache Kafka](https://kafka.apache.org/)-inspired timeseries data container.

## What is this?

- Essentially a [circular buffer](https://www.wikiwand.com/en/Circular_buffer), internally using `Array`
- Read pointers (`Offset`) are exposed to clients, allowing "pull"-style data consumption, just as in Kafka
- Insert(append), read, update, and delete all take <img src="https://latex.codecogs.com/gif.latex?O(1)" title="O(1)" />
- A buffer is made of multiple `Segment`s. Buffer size (= number of `Segment`s and size of each `Segment`) can be configured
- When a whole buffer is filled up, a new "cycle" begins and old `Segment`s are evicted one by one

## Development

Install Elm Platform.

```sh
$ elm-package install
$ elm-test                    # full test
$ elm-test tests/MainTest.elm # only light-weight tests
```

## License

&copy; Copyright 2018 [Yu Matsuzawa](/ymtszw)

BSD-3-Clause
