module Broker.Internal
    exposing
        ( Segments
        , Config
        , config
        , Cycle
        , cycle
        , SegmentIndex
        , segmentIndex
        , InnerOffset
        , innerOffset
        , initSegments
        , capacity
        , append
        , isEmpty
        , oldestReadableOffset
        , offsetToString
        )

import Array exposing (Array)
import Hex


-- TYPES


type alias BrokerInternal a =
    { config : Config
    , segments : Segments a
    , cycle : Cycle
    , segmentIndex : SegmentIndex
    , innerOffset : InnerOffset
    }


type alias Segments a =
    Array (Segment a)



-- Here we introduce two two-state types,
-- in order to differentiate segment states from Maybe values returned by Array APIs.


type Segment a
    = NotInitialized
    | Segment (Array (Item a))


type Item a
    = Empty
    | Item a


type Config
    = Config NumSegments SegmentSize


config : Int -> Int -> Config
config rawNumSegments rawSegmentSize =
    Config (numSegments rawNumSegments) (segmentSize rawSegmentSize)


type NumSegments
    = NumSegments Int


minNumSegments : Int
minNumSegments =
    2


maxNumSegments : Int
maxNumSegments =
    100


numSegments : Int -> NumSegments
numSegments raw =
    if raw < minNumSegments then
        NumSegments minNumSegments
    else if maxNumSegments < raw then
        NumSegments maxNumSegments
    else
        NumSegments raw


type SegmentSize
    = SegmentSize Int


minSegmentSize : Int
minSegmentSize =
    100


maxSegmentSize : Int
maxSegmentSize =
    100000


segmentSize : Int -> SegmentSize
segmentSize raw =
    if raw < minSegmentSize then
        SegmentSize minSegmentSize
    else if maxSegmentSize < raw then
        SegmentSize maxSegmentSize
    else
        SegmentSize raw


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
initSegments (Config (NumSegments numSegments) (SegmentSize segmentSize)) =
    Array.initialize numSegments (always NotInitialized)


capacity : Config -> Int
capacity (Config (NumSegments numSegments) (SegmentSize segmentSize)) =
    numSegments * segmentSize


{-| Append an item to Broker.
-}
append : a -> BrokerInternal a -> BrokerInternal a
append item { config, segments, cycle, segmentIndex, innerOffset } =
    let
        ( nextCycle, nextSegmentIndex, nextInnerOffset ) =
            incrementOffset config cycle segmentIndex innerOffset
    in
        BrokerInternal
            config
            (setInSegments config segmentIndex innerOffset item segments)
            nextCycle
            nextSegmentIndex
            nextInnerOffset


{-| Increment an Offset to the next position in ordinary cases.

Does not take account of Broker reconfiguration;
a Config is considered a static value here.

Also it assumes all numerical values wrapped in value type constructors
are generated from smart cunstructor functions (e.g. `segmentIndex` in this module),
thus within-bound.

-}
incrementOffset :
    Config
    -> Cycle
    -> SegmentIndex
    -> InnerOffset
    -> ( Cycle, SegmentIndex, InnerOffset )
incrementOffset (Config (NumSegments numSegments) (SegmentSize segmentSize)) (Cycle currentCycle) (SegmentIndex currentSegmentIndex) (InnerOffset currentInnerOffset) =
    if currentInnerOffset < segmentSize - 1 then
        ( Cycle currentCycle, SegmentIndex currentSegmentIndex, InnerOffset (currentInnerOffset + 1) )
    else if currentSegmentIndex < numSegments - 1 then
        ( Cycle currentCycle, SegmentIndex (currentSegmentIndex + 1), InnerOffset 0 )
    else
        ( Cycle (currentCycle + 1), SegmentIndex 0, InnerOffset 0 )


setInSegments : Config -> SegmentIndex -> InnerOffset -> a -> Segments a -> Segments a
setInSegments (Config _ segmentSize) (SegmentIndex segmentIndex) (InnerOffset innerOffset) item segments =
    let
        newSegment =
            case Array.get segmentIndex segments of
                Just (Segment segment) ->
                    Segment <| Array.set innerOffset (Item item) segment

                Just NotInitialized ->
                    -- Lazily initialize a segment.
                    -- Must be: innerOffset == 0
                    initSegmentWithFirstItem segmentSize item

                Nothing ->
                    -- segmentIndex out of bound, this should not happen
                    initSegmentWithFirstItem segmentSize item
    in
        Array.set segmentIndex newSegment segments


initSegmentWithFirstItem : SegmentSize -> a -> Segment a
initSegmentWithFirstItem (SegmentSize segmentSize) item =
    Segment <|
        Array.initialize segmentSize <|
            \index ->
                case index of
                    0 ->
                        Item item

                    _ ->
                        Empty


isEmpty : BrokerInternal a -> Bool
isEmpty { segments } =
    case Array.get 0 segments of
        Just NotInitialized ->
            True

        Just (Segment _) ->
            False

        Nothing ->
            -- Should not happen
            True


oldestReadableOffset : BrokerInternal a -> Maybe ( Cycle, SegmentIndex, InnerOffset )
oldestReadableOffset ({ cycle, segmentIndex, innerOffset } as broker) =
    if isEmpty broker then
        Nothing
    else
        case cycle of
            Cycle 0 ->
                Just ( cycle, SegmentIndex 0, InnerOffset 0 )

            Cycle pos ->
                -- Negative cycle should not happen
                Just ( Cycle (pos - 1), segmentIndex, innerOffset )


offsetToString : ( Cycle, SegmentIndex, InnerOffset ) -> String
offsetToString ( Cycle cycle, SegmentIndex segmentIndex, InnerOffset innerOffset ) =
    -- Cycle does not have max; up to 32bits
    (zeroPaddedHex 8 cycle)
        -- SegmentIndex is up to 100 < 8bits
        ++ (zeroPaddedHex 2 segmentIndex)
        -- InnerOffset is up to 100000 < 20bits
        ++ (zeroPaddedHex 5 innerOffset)


zeroPaddedHex : Int -> Int -> String
zeroPaddedHex digits =
    Hex.toString >> String.padLeft digits '0'
