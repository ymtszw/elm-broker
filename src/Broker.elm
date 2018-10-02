module Broker exposing
    ( Broker, Offset
    , initialize, append, read, readOldest, get, update
    , decoder, encode
    , capacity, isEmpty, oldestReadableOffset, nextOffsetToWrite, offsetToString, offsetFromString
    )

{-| Apache Kafka-inspired data stream buffer.


## Types

@docs Broker, Offset


## APIs

@docs initialize, append, read, readOldest, get, update


## Decoder/Encoder

@docs decoder, encode


## Monitoring means

@docs capacity, isEmpty, oldestReadableOffset, nextOffsetToWrite, offsetToString, offsetFromString

-}

import Broker.Internal as I
import Json.Decode as D exposing (Decoder)
import Json.Encode as E



-- TYPES


{-| Data stream buffer.
-}
type Broker a
    = Broker (I.BrokerInternal a)


{-| Global offset within a `Broker`.

Offset itself can live independently from its generating `Broker`.

-}
type Offset
    = Offset I.OffsetInternal



-- APIs


{-| Initializes a `Broker` with a set of configuration.

  - `numSegments` - Number of internal segments. 2 &le; `numSegments` &le; 100
  - `segmentSize` - Size of each segment. 100 &le; `segmentSize` &le; 100,000

Constraints for these configurations are not yet studied well.

-}
initialize : { numSegments : Int, segmentSize : Int } -> Broker a
initialize { numSegments, segmentSize } =
    let
        config_ =
            I.configCtor numSegments segmentSize
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

Also usable when you need to store and reload consumer offset.

-}
offsetToString : Offset -> String
offsetToString (Offset offsetInternal) =
    I.offsetToString offsetInternal


{-| Tries to convert a `String` into `Offset`.

Can be used to reload consumer offset from external storage.

Make sure that a reloaded `Offset` is used against the very `Broker`
that produced that `Offset`.

-}
offsetFromString : String -> Maybe Offset
offsetFromString str =
    Maybe.map Offset (I.offsetFromString str)


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


{-| Decode JS value into Broker. You must supply Decoder for items.

Paired with `encode`, you can "dump and reload" an exisiting Broker.

-}
decoder : Decoder a -> Decoder (Broker a)
decoder itemDecoder =
    D.map Broker (I.decoder itemDecoder)


{-| Encode Broker into JS value. You must supply encode function for items.

Paired with `decoder`, you can "dump and reload" an exisitng Broker.

Do note that, this function naively encodes internal structure of Broker into JS values,
which may require non-ignorable amount of work (both in encode and decode) if capacity of the Broker is big.
More sophisticated "resume" behavior might be needed later.

-}
encode : (a -> E.Value) -> Broker a -> E.Value
encode encodeItem (Broker broker) =
    I.encode encodeItem broker
