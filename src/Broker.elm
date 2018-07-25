module Broker exposing (Broker, Offset, initialize)

{-| Apache Kafka-inspired timeseries data container.


# Types

@docs Broker, Offset


# APIs

@docs initialize

-}

import Broker.Internal exposing (..)


-- TYPES


{-| Timeseries data container.
-}
type Broker a
    = Broker
        { config : Config
        , segments : Segments a
        , cycle : Cycle
        , segmentIndex : SegmentIndex
        , innerOffset : InnerOffset
        }


{-| Global offset within a Broker.

If the Broker's Config is altered AFTER the Offset was recorded (re-initialized),

-}
type Offset
    = Offset
        { cycle : Cycle
        , segmentIndex : SegmentIndex
        , innerOffset : InnerOffset
        }



-- APIS


{-| Initialize a Broker with a Config.
-}
initialize : Int -> Int -> Broker a
initialize rawNumSegments rawSegmentSize =
    let
        config_ =
            config rawNumSegments rawSegmentSize
    in
        Broker
            { config = config_
            , segments = initSegments config_
            , cycle = cycle 0
            , segmentIndex = segmentIndex config_ 0
            , innerOffset = innerOffset config_ 0
            }
