module Broker
    exposing
        ( Broker
        , Offset
        , initialize
        , append
        , capacity
        , isEmpty
        , oldestReadableOffset
        , nextOffsetToWrite
        , offsetToString
        , read
        , readOldest
        )

{-| Apache Kafka-inspired timeseries data container.


## Types

@docs Broker, Offset


## APIs

@docs initialize, append, read, readOldest


## Monitoring means

@docs capacity, isEmpty, oldestReadableOffset, nextOffsetToWrite, offsetToString

-}

import Broker.Internal as I


-- TYPES


{-| Timeseries data container.
-}
type Broker a
    = Broker
        { config : I.Config
        , segments : I.Segments a
        , oldestReadableOffset : Maybe Offset
        , oldestUpdatableOffset : Offset
        , offsetToWrite : Offset
        }


{-| Global offset within a `Broker`.

Offset itself can live independently from its generating `Broker`.

-}
type alias Offset =
    ( I.Cycle, I.SegmentIndex, I.InnerOffset )



-- Internal type is represented as tuple for easier pattern match.
-- APIS


{-| Initializes a `Broker` with a set of configuration.
-}
initialize : Int -> Int -> Broker a
initialize rawNumSegments rawSegmentSize =
    let
        config_ =
            I.config rawNumSegments rawSegmentSize
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
oldestReadableOffset (Broker { oldestReadableOffset }) =
    oldestReadableOffset


{-| Returns an `Offset` that next item will be written to.
-}
nextOffsetToWrite : Broker a -> Offset
nextOffsetToWrite (Broker { offsetToWrite }) =
    offsetToWrite


{-| Converts an `Offset` into a sortable `String` representation.
-}
offsetToString : Offset -> String
offsetToString offset =
    I.offsetToString offset


{-| Read a `Broker` from an `Offset`, returning an item and the next `Offset`,
or `Nothing` if all items are consumed.

If the `Offset` is too old and the target `Segment` is already evicted,
returns oldest readable item with `Offset` next from it.

Currently it assumes Brokers cannot be reconfigured.
This means, if the `Offset` is produced from the same `Broker`,
it can never overtake the current write pointer or become out of bound of the `Broker`.

-}
read : Offset -> Broker a -> Maybe ( a, Offset )
read offset (Broker broker) =
    I.read offset broker


{-| Read a `Broker` from the oldest item. Returns an item and the next `Offset`,
or `Nothing` if the `Broker` is empty.
-}
readOldest : Broker a -> Maybe ( a, Offset )
readOldest (Broker broker) =
    I.readOldest broker
