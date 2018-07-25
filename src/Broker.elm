module Broker exposing (Broker, Offset, initialize, capacity)

{-| Apache Kafka-inspired timeseries data container.


# Types

@docs Broker, Offset


# APIs

@docs initialize, capacity

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

If the Broker's Config is altered AFTER the Offset was recorded (re-initialized),

-}
type Offset
    = Offset
        { cycle : I.Cycle
        , segmentIndex : I.SegmentIndex
        , innerOffset : I.InnerOffset
        }



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
