module Broker exposing
    ( Broker, Offset
    , initialize, append, read, readOldest, get, update
    , capacity, isEmpty, oldestReadableOffset, nextOffsetToWrite, offsetToString
    )

{-| Apache Kafka-inspired timeseries data container.


## Types

@docs Broker, Offset


## APIs

@docs initialize, append, read, readOldest, get, update


## Monitoring means

@docs capacity, isEmpty, oldestReadableOffset, nextOffsetToWrite, offsetToString

-}

import Broker.Internal as I



-- TYPES


{-| Data stream buffer.
-}
type Broker a
    = Broker
        { config : I.Config
        , segments : I.Segments a
        , oldestReadableOffset : Maybe I.OffsetInternal
        , oldestUpdatableOffset : I.OffsetInternal
        , offsetToWrite : I.OffsetInternal
        }


{-| Global offset within a `Broker`.

Offset itself can live independently from its generating `Broker`.

-}
type Offset
    = Offset I.OffsetInternal



-- Internal type is represented as tuple for easier pattern match.
-- APIS


{-| Initializes a `Broker` with a set of configuration.
-}
initialize : Int -> Int -> Broker a
initialize rawNumSegments rawSegmentSize =
    let
        config_ =
            I.configCtor rawNumSegments rawSegmentSize
    in
    Broker
        { config = config_
        , segments = I.initSegments config_
        , oldestReadableOffset = Nothing
        , oldestUpdatableOffset = I.originOffset
        , offsetToWrite = I.originOffset
        }


{-| Returns capacity (number of possible elements) of the Broker.
-}
capacity : Broker a -> Int
capacity (Broker { config }) =
    I.capacity config


{-| Returns whether a Broker is empty or not.
-}
isEmpty : Broker a -> Bool
isEmpty (Broker broker) =
    I.isEmpty broker


{-| Append an item to a Broker.
-}
append : a -> Broker a -> Broker a
append element (Broker broker) =
    Broker (I.append element broker)


{-| Returns the oldest readable `Offset` of a `Broker`. Items older than this `Offset` are already evicted.

If the `Broker` is yet empty, returns `Nothing`.

-}
oldestReadableOffset : Broker a -> Maybe Offset
oldestReadableOffset (Broker broker) =
    Maybe.map Offset broker.oldestReadableOffset


{-| Returns an `Offset` that next item will be written to.
-}
nextOffsetToWrite : Broker a -> Offset
nextOffsetToWrite (Broker { offsetToWrite }) =
    Offset offsetToWrite


{-| Converts an `Offset` into a sortable `String` representation.
-}
offsetToString : Offset -> String
offsetToString (Offset offsetInternal) =
    I.offsetToString offsetInternal


{-| Read a `Broker` by supplying previously read `Offset` (consumer offset),
returning a next item and its `Offset`, or `Nothing` if all items are consumed.

If the `Offset` is too old and the target segment is already evicted, returns the oldest readable item.

Currently it assumes Brokers cannot be reconfigured.
This means, if the `Offset` is produced from the same `Broker`,
it can never overtake the current write pointer or become out of bound of the `Broker`.

-}
read : Offset -> Broker a -> Maybe ( a, Offset )
read (Offset offsetInternal) (Broker broker) =
    Maybe.map (Tuple.mapSecond Offset) (I.read offsetInternal broker)


{-| Read a `Broker` from the oldest item. Returns an item and its `Offset`,
or `Nothing` if the `Broker` is empty.
-}
readOldest : Broker a -> Maybe ( a, Offset )
readOldest (Broker broker) =
    Maybe.map (Tuple.mapSecond Offset) (I.readOldest broker)


{-| Get an item exactly at an `Offset`.

Returns `Nothing` if target segment is already evicted or somehow invalid.

-}
get : Offset -> Broker a -> Maybe a
get (Offset offsetInternal) (Broker broker) =
    I.get offsetInternal broker


{-| Update an item at an `Offset` of a `Broker`.

If target segment is already evicted or not-updatable (soon-to-be-evicted), the `Broker` kept unchanged.

-}
update : Offset -> (a -> a) -> Broker a -> Broker a
update (Offset offsetInternal) transform (Broker broker) =
    Broker (I.update offsetInternal transform broker)
