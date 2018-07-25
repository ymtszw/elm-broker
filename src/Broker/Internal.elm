module Broker.Internal exposing (Segments, Config, config, Cycle, cycle, SegmentIndex, segmentIndex, InnerOffset, innerOffset, initSegments)

import Array exposing (Array)


-- TYPES


type alias Segments a =
    Array (Maybe (Segment a))


type alias Segment a =
    Array (Maybe a)


type Config
    = Config NumSegments SegmentSize


config : Int -> Int -> Config
config rawNumSegments rawSegmentSize =
    Config (numSegments rawNumSegments) (segmentSize rawSegmentSize)


type NumSegments
    = NumSegments Int


numSegments : Int -> NumSegments
numSegments raw =
    if raw > 0 then
        NumSegments raw
    else
        NumSegments 1


type SegmentSize
    = SegmentSize Int


minSegmentSize : Int
minSegmentSize =
    1000


segmentSize : Int -> SegmentSize
segmentSize raw =
    if raw >= minSegmentSize then
        SegmentSize raw
    else
        SegmentSize minSegmentSize


type Cycle
    = Cycle Int


{-| Return type-safe Cycle from a given Int.

Cycles are just positive integers, which can grow arbitrarily (within the limit of Int).

It basically resets to zero when an Elm app is reset (i.e. reloaded), UNLESS the previous Cycle value
was stored somewhere and restored upon initialization.

-}
cycle : Int -> Cycle
cycle raw =
    if raw >= 0 then
        Cycle raw
    else
        Cycle 0


type SegmentIndex
    = SegmentIndex Int


{-| Return type-safe SegmentIndex from a given Int.

  - If within-bound value is given (including 0), just wrap it with constructor.
  - Out-of-bound values are rounded within bound.
      - E.g. if numSegments is 4, 5 will result in 1, whereas -5 will result in 3)
      - This is just modular arithmetic. Negative values can be used to count from tail.

-}
segmentIndex : Config -> Int -> SegmentIndex
segmentIndex (Config (NumSegments numSegments) _) raw =
    if 0 <= raw && raw < numSegments then
        SegmentIndex raw
    else
        SegmentIndex (raw % numSegments)


type InnerOffset
    = InnerOffset Int


{-| Return type-safe InnerOffset from a given Int. Works just like segmentIndex.
-}
innerOffset : Config -> Int -> InnerOffset
innerOffset (Config _ (SegmentSize segmentSize)) raw =
    if 0 <= raw && raw < segmentSize then
        InnerOffset raw
    else
        InnerOffset (raw % segmentSize)



-- APIS


initSegments : Config -> Segments a
initSegments (Config (NumSegments numSegmentsInt) (SegmentSize segmentSizeInt)) =
    Array.initialize numSegmentsInt <|
        \segmentIndex ->
            case segmentIndex of
                0 ->
                    Just <| Array.initialize segmentSizeInt <| always Nothing

                _ ->
                    Nothing
