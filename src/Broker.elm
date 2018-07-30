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
        )

{-| Apache Kafka-inspired timeseries data container.


## Types

@docs Broker, Offset


## APIs

@docs initialize, append


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
        , cycle : I.Cycle
        , segmentIndex : I.SegmentIndex
        , innerOffset : I.InnerOffset
        }


{-| Global offset within a Broker.

Offset itself can live independently from its generating Broker.

-}
type Offset
    = Offset ( I.Cycle, I.SegmentIndex, I.InnerOffset ) -- Internal type is represented as tuple for easier pattern match.



-- APIS


{-| Initializes a Broker with a Config.
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
            , cycle = I.cycle 0
            , segmentIndex = I.segmentIndex config_ 0
            , innerOffset = I.innerOffset config_ 0
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


{-| Returns an oldest readable Offset of a Broker. Items older than this Offset are already evicted.

If the Broker is yet empty, returns Nothing.

-}
oldestReadableOffset : Broker a -> Maybe Offset
oldestReadableOffset (Broker broker) =
    I.oldestReadableOffset broker |> Maybe.map Offset


{-| Returns an Offset that next item will be written to.
-}
nextOffsetToWrite : Broker a -> Offset
nextOffsetToWrite (Broker { cycle, segmentIndex, innerOffset }) =
    Offset ( cycle, segmentIndex, innerOffset )


{-| Converts an Offset into a sortable String representation.
-}
offsetToString : Offset -> String
offsetToString (Offset offset) =
    I.offsetToString offset
