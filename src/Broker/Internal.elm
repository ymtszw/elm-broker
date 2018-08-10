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
        , originOffset
        , capacity
        , append
        , isEmpty
        , offsetToString
        , read
        , readOldest
        , update
        )

import Array exposing (Array)
import Hex


-- TYPES


type alias BrokerInternal a =
    { config : Config
    , segments : Segments a
    , oldestReadableOffset : Maybe OffsetInternal
    , oldestUpdatableOffset : OffsetInternal
    , offsetToWrite : OffsetInternal
    }


type alias Segments a =
    { active : Array (Segment a)
    , fading : Maybe (Segment a)
    }


{-| Here we introduce two two-state types, Segment and Item,
in order to differentiate segment states from Maybe values returned by Array APIs.
-}
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


type alias OffsetInternal =
    ( Cycle, SegmentIndex, InnerOffset )


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
    { active = Array.initialize numSegments (always NotInitialized)
    , fading = Nothing
    }


originOffset : OffsetInternal
originOffset =
    ( Cycle 0, SegmentIndex 0, InnerOffset 0 )


capacity : Config -> Int
capacity (Config (NumSegments numSegments) (SegmentSize segmentSize)) =
    numSegments * segmentSize


{-| Append an item to a Broker.

When Cycle is renewed (all Segments are filled up), old Segments will "lazily" enter "fading" state one by one, become unupdatable.
By "lazily" here means that until a new item actually arrives for a new Segment (in a new Cycle),
Soon-to-be-"fading" Segment is still considered "active" AND currently "fading" Segment is still there, readable.

When the new item arrives, the old Segment will enter "fading" state and previously "fading" Segment is now completely gone, unreadable.

-}
append : a -> BrokerInternal a -> BrokerInternal a
append item ({ config, segments, oldestReadableOffset, oldestUpdatableOffset, offsetToWrite } as broker) =
    let
        nextOffsetToWrite =
            incrementOffset config offsetToWrite

        offsetUpdatedBroker =
            case nextOffsetToWrite of
                ( Cycle 0, SegmentIndex 0, InnerOffset 1 ) ->
                    -- Origin Segment now becomes readaable
                    { broker | oldestReadableOffset = Just originOffset }

                ( Cycle 0, _, _ ) ->
                    -- In Cycle 0, no eviction happens
                    broker

                ( Cycle 1, SegmentIndex 0, InnerOffset 1 ) ->
                    -- Cycle 1 (1-0) starts to be filled, now 0-0 Segment enter "fading" state, but still readable
                    { broker | oldestUpdatableOffset = forwardToNextSegment config oldestUpdatableOffset }

                ( Cycle _, SegmentIndex _, InnerOffset 1 ) ->
                    -- From here on, "oldest" Offsets are incremented in-sync when innerOffset counts 1 (first item is written to new Segment)
                    -- Previous oldestUpdatableOffset always becomes next oldestReadableOffset
                    { broker
                        | oldestReadableOffset = Just oldestUpdatableOffset
                        , oldestUpdatableOffset = forwardToNextSegment config oldestUpdatableOffset
                    }

                ( Cycle _, SegmentIndex _, InnerOffset _ ) ->
                    broker
    in
        { offsetUpdatedBroker
            | segments = appendToSegments config offsetToWrite item segments
            , offsetToWrite = nextOffsetToWrite
        }


{-| Increment an Offset to the next position in ordinary cases.

Does not take account for Broker reconfiguration;
a Config is considered a static value here.

Also it assumes all numerical values wrapped in value type constructors
are generated from smart cunstructor functions (e.g. `segmentIndex` in this module),
thus within-bound.

-}
incrementOffset : Config -> OffsetInternal -> OffsetInternal
incrementOffset (Config (NumSegments numSegments) (SegmentSize segmentSize)) ( Cycle currentCycle, SegmentIndex currentSegmentIndex, InnerOffset currentInnerOffset ) =
    if currentInnerOffset < segmentSize - 1 then
        ( Cycle currentCycle, SegmentIndex currentSegmentIndex, InnerOffset (currentInnerOffset + 1) )
    else if currentSegmentIndex < numSegments - 1 then
        ( Cycle currentCycle, SegmentIndex (currentSegmentIndex + 1), InnerOffset 0 )
    else
        ( Cycle (currentCycle + 1), SegmentIndex 0, InnerOffset 0 )


forwardToNextSegment : Config -> OffsetInternal -> OffsetInternal
forwardToNextSegment (Config (NumSegments numSegments) _) ( Cycle currentCycle, SegmentIndex currentSegmentIndex, _ ) =
    if currentSegmentIndex < numSegments - 1 then
        ( Cycle currentCycle, SegmentIndex (currentSegmentIndex + 1), InnerOffset 0 )
    else
        ( Cycle (currentCycle + 1), SegmentIndex 0, InnerOffset 0 )


{-| Set a value in Segments at an Offset. If the Offset is stepping into next SegmentIndex, evict the old Segment.

The evicted Segment will enter "fading" state, and currently-"fading" Segment will be gone.

In future, "fading" Segment may be applied to "onEvicted" callback which can be given by users,
allowing custom handling of old data (e.g. dump to IndexedDB, upload to somewhere, etc.)

-}
appendToSegments : Config -> OffsetInternal -> a -> Segments a -> Segments a
appendToSegments (Config _ segmentSize) ( _, SegmentIndex segmentIndex, InnerOffset innerOffset ) item ({ active, fading } as segments) =
    let
        setActive newSegment ({ active } as segments) =
            { segments | active = Array.set segmentIndex newSegment active }
    in
        case Array.get segmentIndex active of
            Just ((Segment segment) as oldSegment) ->
                -- Segment exists, either already initialized, or stepped into existing one after full Cycle
                if innerOffset == 0 then
                    -- Stepped into next Segment, so evict old one
                    -- At this moment previously "faded" Segment will be gone
                    { segments | fading = Just oldSegment }
                        |> setActive (initSegmentWithFirstItem segmentSize item)
                else
                    setActive (Segment (Array.set innerOffset (Item item) segment)) segments

            Just NotInitialized ->
                -- Lazily initialize a segment.
                -- Must be: innerOffset == 0
                segments
                    |> setActive (initSegmentWithFirstItem segmentSize item)

            Nothing ->
                -- segmentIndex out of bound, this should not happen
                segments
                    |> setActive (initSegmentWithFirstItem segmentSize item)


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
isEmpty { oldestReadableOffset } =
    case oldestReadableOffset of
        Just _ ->
            False

        Nothing ->
            True


offsetToString : OffsetInternal -> String
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


offsetOlderThan : OffsetInternal -> OffsetInternal -> Bool
offsetOlderThan ( Cycle c1, SegmentIndex s1, InnerOffset i1 ) ( Cycle c2, SegmentIndex s2, InnerOffset i2 ) =
    ( c1, s1, i1 ) < ( c2, s2, i2 )


read : OffsetInternal -> BrokerInternal a -> Maybe ( a, OffsetInternal )
read consumerOffset ({ config, oldestReadableOffset } as broker) =
    case oldestReadableOffset of
        Just oro ->
            if offsetIsValid config consumerOffset then
                readIfTargetOffsetIsReadable oro (incrementOffset config consumerOffset) broker
            else
                -- Shuold not happen as long as Offsets are produced from targeting Broker
                Nothing

        Nothing ->
            -- Broker is empty
            Nothing


{-| Check if an `Offset` is within bounds of a `Broker`'s `Config`.

Does not check if the `Offset` is not evicted or not overtaking write pointer.

-}
offsetIsValid : Config -> OffsetInternal -> Bool
offsetIsValid (Config (NumSegments numSegments) (SegmentSize segmentSize)) ( _, SegmentIndex segmentIndex, InnerOffset innerOffset ) =
    -- Skipping 0 <= segmentIndex and 0 <= innerOffset assertion since they are super unlikely to be False
    (segmentIndex < numSegments) && (innerOffset < segmentSize)


readIfTargetOffsetIsReadable : OffsetInternal -> OffsetInternal -> BrokerInternal a -> Maybe ( a, OffsetInternal )
readIfTargetOffsetIsReadable oldestReadableOffset targetOffset broker =
    if offsetOlderThan targetOffset oldestReadableOffset then
        readAtSurelyReadableOffset oldestReadableOffset broker
    else if offsetOlderThan targetOffset broker.offsetToWrite then
        readAtSurelyReadableOffset targetOffset broker
    else
        -- The Offset equals to current write pointer, OR somehow overtook it
        Nothing


readAtSurelyReadableOffset : OffsetInternal -> BrokerInternal a -> Maybe ( a, OffsetInternal )
readAtSurelyReadableOffset surelyReadableOffset broker =
    broker
        |> getFromSegmentsAtSurelyReadableOffset surelyReadableOffset
        |> Maybe.map (\item -> ( item, surelyReadableOffset ))


getFromSegmentsAtSurelyReadableOffset : OffsetInternal -> BrokerInternal a -> Maybe a
getFromSegmentsAtSurelyReadableOffset (( _, _, innerOffset ) as surelyReadableOffset) { segments, oldestUpdatableOffset } =
    if not (offsetOlderThan surelyReadableOffset oldestUpdatableOffset) then
        getFromActiveSegmentsAtSurelyReadableOffset surelyReadableOffset segments.active
    else
        case segments.fading of
            Just (Segment fading) ->
                getFromSegmentAtSurelyReadableInnerOffset innerOffset fading

            Just NotInitialized ->
                -- Should not happen; There must be a Segment on readable segmentIndex
                Nothing

            Nothing ->
                -- Should not happen; if a readable Offset is older than the oldest updatable Offset, there must be a fading Segment
                Nothing


getFromActiveSegmentsAtSurelyReadableOffset : OffsetInternal -> Array (Segment a) -> Maybe a
getFromActiveSegmentsAtSurelyReadableOffset ( _, SegmentIndex segmentIndex, innerOffset ) active =
    case Array.get segmentIndex active of
        Just (Segment targetSegment) ->
            getFromSegmentAtSurelyReadableInnerOffset innerOffset targetSegment

        _ ->
            -- Should not happen; There must be a Segment at readable segmentIndex
            Nothing


getFromSegmentAtSurelyReadableInnerOffset : InnerOffset -> Array (Item a) -> Maybe a
getFromSegmentAtSurelyReadableInnerOffset (InnerOffset innerOffset) segment =
    case Array.get innerOffset segment of
        Just (Item a) ->
            Just a

        _ ->
            -- Should not happen; There must be an Item at readable innerOffset
            Nothing


readOldest : BrokerInternal a -> Maybe ( a, OffsetInternal )
readOldest broker =
    broker.oldestReadableOffset
        |> Maybe.andThen (\oro -> readAtSurelyReadableOffset oro broker)


update : OffsetInternal -> (a -> a) -> BrokerInternal a -> BrokerInternal a
update targetOffset transform ({ segments, oldestUpdatableOffset } as broker) =
    if offsetOlderThan targetOffset oldestUpdatableOffset then
        broker
    else
        { broker | segments = updateInSegments targetOffset transform segments }


updateInSegments : OffsetInternal -> (a -> a) -> Segments a -> Segments a
updateInSegments ( _, SegmentIndex segmentIndex, InnerOffset innerOffset ) transform ({ active } as segments) =
    case Array.get segmentIndex active of
        Just (Segment segment) ->
            case updateItemInSegment innerOffset transform segment of
                Just newSegment ->
                    { segments | active = Array.set segmentIndex newSegment active }

                Nothing ->
                    segments

        Just NotInitialized ->
            -- Should not happen
            segments

        Nothing ->
            -- Should not happen
            segments


updateItemInSegment : Int -> (a -> a) -> Array (Item a) -> Maybe (Segment a)
updateItemInSegment innerOffset transform segment =
    case Array.get innerOffset segment of
        Just (Item item) ->
            Just <|
                Segment <|
                    Array.set innerOffset (Item (transform item)) segment

        Just Empty ->
            -- Unlikely to happen
            Nothing

        Nothing ->
            Nothing
