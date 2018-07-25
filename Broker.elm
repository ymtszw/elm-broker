module Broker exposing (Broker, Offset, initialize)

{-| Apache Kafka-inspired timeseries data container.


# Types

@docs Broker, Offset


# APIs

@docs initialize

-}

import Array exposing (Array)


{-| Timeseries data container.
-}
type Broker a
    = Broker
        { config : Config
        , segments : Array (Maybe (Segment a))
        , cycle : Cycle
        }


type alias Config =
    { numSegments : NumSegments
    , segmentSize : SegmentSize
    }


type alias Segment a =
    Array a


type Cycle
    = Cycle Int


{-| Global offset within a Broker.
-}
type Offset
    = Offset
        { config : Config
        , cycle : Cycle
        , segmentIndex : SegmentIndex
        , innerOffset : InnerOffset
        }


{-| Initialize a Broker with a Config.
-}
initialize : Config -> Broker a
initialize config =
    Broker
        { config = config
        , segments = initializeSegments config
        , cycle = initializeCycle
        }
