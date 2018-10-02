module Broker.Internal exposing
    ( BrokerInternal, Config, configCtor, Segments, initSegments
    , OffsetInternal, Cycle, SegmentIndex, InnerOffset, originOffset
    , encode, decoder
    , append, read, readOldest, get, update, isEmpty, capacity, offsetToString
    )

{-| Internal module.


## Types

@docs BrokerInternal, Config, configCtor, Segments, initSegments


## Offsets

@docs OffsetInternal, Cycle, SegmentIndex, InnerOffset, originOffset


## Decoder/Encoder

@docs encode, decoder


## APIs

@docs append, read, readOldest, get, update, isEmpty, capacity, offsetToString

-}

import Array exposing (Array)
import Json.Decode as D exposing (Decoder)
import Json.Encode as E



-- TYPES


type alias BrokerInternal a =
    { config : Config
    , segments : Segments a
    , oldestReadableOffset : Maybe OffsetInternal
    , oldestUpdatableOffset : OffsetInternal
    , offsetToWrite : OffsetInternal
    }


encode : (a -> E.Value) -> BrokerInternal a -> E.Value
encode encodeItem broker =
    E.object
        [ ( "config", encodeConfig broker.config )
        , ( "segments", encodeSegments encodeItem broker.segments )
        , ( "oldestReadableOffset", Maybe.withDefault E.null (Maybe.map encodeOffset broker.oldestReadableOffset) )
        , ( "oldestUpdatableOffset", encodeOffset broker.oldestUpdatableOffset )
        , ( "offsetToWrite", encodeOffset broker.offsetToWrite )
        ]


decoder : Decoder a -> Decoder (BrokerInternal a)
decoder itemDecoder =
    D.map5 BrokerInternal
        (D.field "config" configDecoder)
        (D.field "segments" (segmentsDecoder itemDecoder))
        (D.field "oldestReadableOffset" (D.maybe offsetDecoder))
        (D.field "oldestUpdatableOffset" offsetDecoder)
        (D.field "offsetToWrite" offsetDecoder)


type alias Segments a =
    { active : Array (Segment a)
    , fading : Maybe (Segment a)
    }


encodeSegments : (a -> E.Value) -> Segments a -> E.Value
encodeSegments encodeItem segments =
    E.object
        [ ( "active", E.array (encodeSegment encodeItem) segments.active )
        , ( "fading", Maybe.withDefault E.null (Maybe.map (encodeSegment encodeItem) segments.fading) )
        ]


segmentsDecoder : Decoder a -> Decoder (Segments a)
segmentsDecoder itemDecoder =
    D.map2 Segments
        (D.field "active" (D.array (segmentDecoder itemDecoder)))
        (D.field "fading" (D.maybe (segmentDecoder itemDecoder)))


{-| Here we introduce two two-state types, Segment and Item,
in order to differentiate segment states from Maybe values returned by Array APIs.
-}
type Segment a
    = NotInitialized
    | Segment (Array (Item a))


encodeSegment : (a -> E.Value) -> Segment a -> E.Value
encodeSegment encodeItem segment =
    case segment of
        NotInitialized ->
            E.string "NotInitialized"

        Segment itemArray ->
            let
                encodeArrayItem item =
                    case item of
                        Empty ->
                            E.object [ ( "tag", E.string "Empty" ) ]

                        Item a ->
                            E.object [ ( "tag", E.string "Item" ), ( "item", encodeItem a ) ]
            in
            E.array encodeArrayItem itemArray


segmentDecoder : Decoder a -> Decoder (Segment a)
segmentDecoder itemDecoder =
    let
        arrayItemDecoder =
            D.field "tag" D.string
                |> D.andThen
                    (\tag ->
                        case tag of
                            "Empty" ->
                                D.succeed Empty

                            "Item" ->
                                D.map Item (D.field "item" itemDecoder)

                            _ ->
                                D.fail ("Corrupted Segment Item tag: " ++ tag)
                    )
    in
    D.oneOf
        [ D.andThen
            (\tag ->
                if tag == "NotInitialized" then
                    D.succeed NotInitialized

                else
                    D.fail ("Corrupted Segment tag: " ++ tag)
            )
            D.string
        , D.map Segment (D.array arrayItemDecoder)
        ]


type Item a
    = Empty
    | Item a


type Config
    = Config NumSegments SegmentSize


configCtor : Int -> Int -> Config
configCtor numSegmentsInt segmentSizeInt =
    Config (numSegmentsCtor numSegmentsInt) (segmentSizeCtor segmentSizeInt)


encodeConfig : Config -> E.Value
encodeConfig (Config (NumSegments numSegments) (SegmentSize segmentSize)) =
    E.object [ ( "numSegments", E.int numSegments ), ( "segmentSize", E.int segmentSize ) ]


configDecoder : Decoder Config
configDecoder =
    D.map2 Config
        (D.field "numSegments" (D.map NumSegments D.int))
        (D.field "segmentSize" (D.map SegmentSize D.int))


type NumSegments
    = NumSegments Int


numSegmentsCtor : Int -> NumSegments
numSegmentsCtor raw =
    if raw < minNumSegments then
        NumSegments minNumSegments

    else if maxNumSegments < raw then
        NumSegments maxNumSegments

    else
        NumSegments raw


minNumSegments : Int
minNumSegments =
    2


maxNumSegments : Int
maxNumSegments =
    100


type SegmentSize
    = SegmentSize Int


segmentSizeCtor : Int -> SegmentSize
segmentSizeCtor raw =
    if raw < minSegmentSize then
        SegmentSize minSegmentSize

    else if maxSegmentSize < raw then
        SegmentSize maxSegmentSize

    else
        SegmentSize raw


minSegmentSize : Int
minSegmentSize =
    100


maxSegmentSize : Int
maxSegmentSize =
    100000


type alias OffsetInternal =
    ( Cycle, SegmentIndex, InnerOffset )


encodeOffset : OffsetInternal -> E.Value
encodeOffset ( Cycle cycle, SegmentIndex segmentIndex, InnerOffset innerOffset ) =
    E.object
        [ ( "cycle", E.int cycle )
        , ( "segmentIndex", E.int segmentIndex )
        , ( "innerOffset", E.int innerOffset )
        ]


offsetDecoder : Decoder OffsetInternal
offsetDecoder =
    D.map3 (\a b c -> ( a, b, c ))
        (D.field "cycle" (D.map Cycle D.int))
        (D.field "segmentIndex" (D.map SegmentIndex D.int))
        (D.field "innerOffset" (D.map InnerOffset D.int))


type Cycle
    = Cycle Int


type SegmentIndex
    = SegmentIndex Int


type InnerOffset
    = InnerOffset Int



-- APIS


initSegments : Config -> Segments a
initSegments (Config (NumSegments numSegmentInt) _) =
    { active = Array.initialize numSegmentInt (always NotInitialized)
    , fading = Nothing
    }


originOffset : OffsetInternal
originOffset =
    ( Cycle 0, SegmentIndex 0, InnerOffset 0 )


capacity : Config -> Int
capacity (Config (NumSegments numSegmentInt) (SegmentSize segmentSizeInt)) =
    numSegmentInt * segmentSizeInt


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
incrementOffset (Config (NumSegments numSegmentInt) (SegmentSize segmentSizeInt)) ( Cycle cycleInt, SegmentIndex segmentIndexInt, InnerOffset innerOffsetInt ) =
    if innerOffsetInt < segmentSizeInt - 1 then
        ( Cycle cycleInt, SegmentIndex segmentIndexInt, InnerOffset (innerOffsetInt + 1) )

    else if segmentIndexInt < numSegmentInt - 1 then
        ( Cycle cycleInt, SegmentIndex (segmentIndexInt + 1), InnerOffset 0 )

    else
        ( Cycle (cycleInt + 1), SegmentIndex 0, InnerOffset 0 )


forwardToNextSegment : Config -> OffsetInternal -> OffsetInternal
forwardToNextSegment (Config (NumSegments numSegmentInt) _) ( Cycle cycleInt, SegmentIndex segmentIndexInt, _ ) =
    if segmentIndexInt < numSegmentInt - 1 then
        ( Cycle cycleInt, SegmentIndex (segmentIndexInt + 1), InnerOffset 0 )

    else
        ( Cycle (cycleInt + 1), SegmentIndex 0, InnerOffset 0 )


{-| Set a value in Segments at an Offset. If the Offset is stepping into next SegmentIndex, evict the old Segment.

The evicted Segment will enter "fading" state, and currently-"fading" Segment will be gone.

In future, "fading" Segment may be applied to "onEvicted" callback which can be given by users,
allowing custom handling of old data (e.g. dump to IndexedDB, upload to somewhere, etc.)

-}
appendToSegments : Config -> OffsetInternal -> a -> Segments a -> Segments a
appendToSegments (Config _ segmentSizeInt) ( _, SegmentIndex segmentIndexInt, InnerOffset innerOffsetInt ) item ({ active, fading } as segments) =
    case Array.get segmentIndexInt active of
        Just ((Segment segment) as oldSegment) ->
            -- Segment exists, either already initialized, or stepped into existing one after full Cycle
            if innerOffsetInt == 0 then
                -- Stepped into next Segment, so evict old one
                -- At this moment previously "faded" Segment will be gone
                { segments | fading = Just oldSegment }
                    |> setActive segmentIndexInt (initSegmentWithFirstItem segmentSizeInt item)

            else
                setActive segmentIndexInt (Segment (Array.set innerOffsetInt (Item item) segment)) segments

        Just NotInitialized ->
            -- Lazily initialize a segment.
            -- Must be: innerOffsetInt == 0
            segments
                |> setActive segmentIndexInt (initSegmentWithFirstItem segmentSizeInt item)

        Nothing ->
            -- segmentIndex out of bound, this should not happen
            segments
                |> setActive segmentIndexInt (initSegmentWithFirstItem segmentSizeInt item)


setActive : Int -> Segment a -> Segments a -> Segments a
setActive segmentIndexInt newSegment ({ active } as segments) =
    { segments | active = Array.set segmentIndexInt newSegment active }


initSegmentWithFirstItem : SegmentSize -> a -> Segment a
initSegmentWithFirstItem (SegmentSize segmentSizeInt) item =
    Segment <|
        Array.initialize segmentSizeInt <|
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
offsetToString ( Cycle cycleInt, SegmentIndex segmentIndexInt, InnerOffset innerOffsetInt ) =
    -- Cycle does not have max; up to 32bits
    zeroPaddedHex 8 cycleInt
        -- SegmentIndex is up to 100 < 8bits
        ++ zeroPaddedHex 2 segmentIndexInt
        -- InnerOffset is up to 100000 < 20bits
        ++ zeroPaddedHex 5 innerOffsetInt


zeroPaddedHex : Int -> Int -> String
zeroPaddedHex digits =
    toHex "" >> String.padLeft digits '0'


toHex : String -> Int -> String
toHex acc num =
    if num < 16 then
        toHexImpl num ++ acc

    else
        toHex (toHexImpl (modBy 16 num) ++ acc) (num // 16)


toHexImpl : Int -> String
toHexImpl numUpTo15 =
    case numUpTo15 of
        0 ->
            "0"

        1 ->
            "1"

        2 ->
            "2"

        3 ->
            "3"

        4 ->
            "4"

        5 ->
            "5"

        6 ->
            "6"

        7 ->
            "7"

        8 ->
            "8"

        9 ->
            "9"

        10 ->
            "a"

        11 ->
            "b"

        12 ->
            "c"

        13 ->
            "d"

        14 ->
            "e"

        15 ->
            "f"

        _ ->
            -- Should not happen; Trapping by infinite loop!
            toHexImpl numUpTo15


offsetOlderThan : OffsetInternal -> OffsetInternal -> Bool
offsetOlderThan ( Cycle c1, SegmentIndex si1, InnerOffset io1 ) ( Cycle c2, SegmentIndex si2, InnerOffset io2 ) =
    ( c1, si1, io1 ) < ( c2, si2, io2 )


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
offsetIsValid (Config (NumSegments numSegmentInt) (SegmentSize segmentSizeInt)) ( _, SegmentIndex segmentIndexInt, InnerOffset innerOffsetInt ) =
    -- Skipping 0 <= segmentIndexInt and 0 <= innerOffsetInt assertion since they are super unlikely to be False
    (segmentIndexInt < numSegmentInt) && (innerOffsetInt < segmentSizeInt)


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
getFromSegmentsAtSurelyReadableOffset (( _, _, innerOffsetInt ) as surelyReadableOffset) { segments, oldestUpdatableOffset } =
    if not (offsetOlderThan surelyReadableOffset oldestUpdatableOffset) then
        getFromActiveSegmentsAtSurelyReadableOffset surelyReadableOffset segments.active

    else
        case segments.fading of
            Just (Segment fading) ->
                getFromSegmentAtSurelyReadableInnerOffset innerOffsetInt fading

            Just NotInitialized ->
                -- Should not happen; There must be a Segment on readable segmentIndex
                Nothing

            Nothing ->
                -- Should not happen; if a readable Offset is older than the oldest updatable Offset, there must be a fading Segment
                Nothing


getFromActiveSegmentsAtSurelyReadableOffset : OffsetInternal -> Array (Segment a) -> Maybe a
getFromActiveSegmentsAtSurelyReadableOffset ( _, SegmentIndex segmentIndexInt, innerOffsetInt ) active =
    case Array.get segmentIndexInt active of
        Just (Segment targetSegment) ->
            getFromSegmentAtSurelyReadableInnerOffset innerOffsetInt targetSegment

        _ ->
            -- Should not happen; There must be a Segment at readable segmentIndex
            Nothing


getFromSegmentAtSurelyReadableInnerOffset : InnerOffset -> Array (Item a) -> Maybe a
getFromSegmentAtSurelyReadableInnerOffset (InnerOffset innerOffsetInt) segment =
    case Array.get innerOffsetInt segment of
        Just (Item a) ->
            Just a

        _ ->
            -- Should not happen; There must be an Item at readable innerOffset
            -- This may change if delete API is introduced
            Nothing


readOldest : BrokerInternal a -> Maybe ( a, OffsetInternal )
readOldest broker =
    broker.oldestReadableOffset
        |> Maybe.andThen (\oro -> readAtSurelyReadableOffset oro broker)


get : OffsetInternal -> BrokerInternal a -> Maybe a
get targetOffset ({ config, oldestReadableOffset, offsetToWrite } as broker) =
    case oldestReadableOffset of
        Just oro ->
            if offsetIsValid config targetOffset then
                if offsetOlderThan targetOffset oro then
                    -- Target Segment is evicted
                    Nothing

                else if offsetOlderThan targetOffset offsetToWrite then
                    getFromSegmentsAtSurelyReadableOffset targetOffset broker

                else
                    -- The Offset equals to current write pointer, OR somehow overtook it
                    Nothing

            else
                -- Shuold not happen as long as Offsets are produced from targeting Broker
                Nothing

        Nothing ->
            -- Broker is empty
            Nothing


update : OffsetInternal -> (a -> a) -> BrokerInternal a -> BrokerInternal a
update targetOffset transform ({ segments, oldestUpdatableOffset } as broker) =
    if offsetOlderThan targetOffset oldestUpdatableOffset then
        broker

    else
        { broker | segments = updateInSegments targetOffset transform segments }


updateInSegments : OffsetInternal -> (a -> a) -> Segments a -> Segments a
updateInSegments ( _, SegmentIndex segmentIndexInt, InnerOffset innerOffsetInt ) transform ({ active } as segments) =
    case Array.get segmentIndexInt active of
        Just (Segment segment) ->
            case updateItemInSegment innerOffsetInt transform segment of
                Just newSegment ->
                    { segments | active = Array.set segmentIndexInt newSegment active }

                Nothing ->
                    segments

        Just NotInitialized ->
            -- Should not happen
            segments

        Nothing ->
            -- Should not happen
            segments


updateItemInSegment : Int -> (a -> a) -> Array (Item a) -> Maybe (Segment a)
updateItemInSegment innerOffsetInt transform segment =
    case Array.get innerOffsetInt segment of
        Just (Item item) ->
            Just <|
                Segment <|
                    Array.set innerOffsetInt (Item (transform item)) segment

        Just Empty ->
            -- Unlikely to happen
            Nothing

        Nothing ->
            Nothing
