module MainExamples exposing (appendUpto, oldestReadableOffsetInString, suite)

import Broker exposing (Broker)
import Expect exposing (Expectation)
import Json.Decode
import Json.Encode
import String exposing (fromInt)
import Test exposing (..)


initializeSuite : Test
initializeSuite =
    describe "initialize should work"
        [ test "empty after initialize" <|
            \_ -> Broker.initialize 2 100 |> Broker.isEmpty |> Expect.true "Expected newly initialized Broker to be empty"
        , testInitialize 2 100 200
        , testInitialize 100 100000 10000000
        , testInitialize 0 100 200
        , testInitialize 1 100 200
        , testInitialize 2 1 200
        , testInitialize 0 1 200
        , testInitialize 1 1 200
        , testInitialize 2 100001 200000
        ]


testInitialize : Int -> Int -> Int -> Test
testInitialize numSegments segmentSize expectCapacity =
    test ("numSegments: " ++ fromInt numSegments ++ ", segmentSize: " ++ fromInt segmentSize) <|
        \_ ->
            Broker.initialize numSegments segmentSize |> Broker.capacity |> Expect.equal expectCapacity


appendSuite : Test
appendSuite =
    describe "append should work indefinitely"
        [ testAppend ( 2, 100 ) 200 ( "00000000" ++ "00" ++ "00000", "00000001" ++ "00" ++ "00000" )
        , testAppend ( 2, 100 ) 300 ( "00000000" ++ "00" ++ "00000", "00000001" ++ "01" ++ "00000" )
        , testAppend ( 10, 200 ) 12345 ( "00000005" ++ "01" ++ "00000", "00000006" ++ "01" ++ "00091" )
        ]


testAppend : ( Int, Int ) -> Int -> ( String, String ) -> Test
testAppend ( numSegments, segmentSize ) upTo ( expectOldest, expectNext ) =
    test ("up to " ++ fromInt upTo ++ " (" ++ fromInt numSegments ++ "*" ++ fromInt segmentSize ++ " capacity)") <|
        \_ ->
            Broker.initialize numSegments segmentSize
                |> appendUpto upTo identity
                |> Expect.all
                    [ oldestReadableOffsetInString >> Expect.equal (Just expectOldest)
                    , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal expectNext
                    ]


oldestReadableOffsetInString : Broker a -> Maybe String
oldestReadableOffsetInString =
    Broker.oldestReadableOffset >> Maybe.map Broker.offsetToString


appendUpto : Int -> (Int -> a) -> Broker a -> Broker a
appendUpto count itemGenerator broker =
    if count <= 0 then
        broker

    else
        appendUpto (count - 1) itemGenerator (Broker.append (itemGenerator count) broker)


fadingSuite200 : Test
fadingSuite200 =
    describe "oldestReadableOffset should work including fading segments (200 capacity)"
        [ test "not appended" <| \_ -> Broker.initialize 2 100 |> oldestReadableOffsetInString |> Expect.equal Nothing
        , testFading ( 2, 100 ) 1 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 199 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 200 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 201 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 299 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 300 ("00000000" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 301 ("00000000" ++ "01" ++ "00000")
        , testFading ( 2, 100 ) 399 ("00000000" ++ "01" ++ "00000")
        , testFading ( 2, 100 ) 400 ("00000000" ++ "01" ++ "00000")
        , testFading ( 2, 100 ) 401 ("00000001" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 499 ("00000001" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 500 ("00000001" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 501 ("00000001" ++ "01" ++ "00000")
        , testFading ( 2, 100 ) 601 ("00000002" ++ "00" ++ "00000")
        , testFading ( 2, 100 ) 701 ("00000002" ++ "01" ++ "00000")
        , testFading ( 2, 100 ) 801 ("00000003" ++ "00" ++ "00000")
        ]


testFading : ( Int, Int ) -> Int -> String -> Test
testFading ( numSegments, segmentSize ) upTo expectOldest =
    test ("up to " ++ fromInt upTo ++ " (" ++ fromInt numSegments ++ "*" ++ fromInt segmentSize ++ " capacity)") <|
        \_ ->
            Broker.initialize numSegments segmentSize
                |> appendUpto upTo identity
                |> oldestReadableOffsetInString
                |> Expect.equal (Just expectOldest)


fadingSuite2000 : Test
fadingSuite2000 =
    describe "oldestReadableOffset should work including fading segments (10 * 200 = 2000 capacity)"
        [ testFading ( 10, 200 ) 2200 ("00000000" ++ "00" ++ "00000")
        , testFading ( 10, 200 ) 2201 ("00000000" ++ "01" ++ "00000")
        , testFading ( 10, 200 ) 2300 ("00000000" ++ "01" ++ "00000")
        , testFading ( 10, 200 ) 2400 ("00000000" ++ "01" ++ "00000")
        , testFading ( 10, 200 ) 2401 ("00000000" ++ "02" ++ "00000")
        , testFading ( 10, 200 ) 4000 ("00000000" ++ "09" ++ "00000")
        , testFading ( 10, 200 ) 4001 ("00000001" ++ "00" ++ "00000")
        , testFading ( 10, 200 ) 22000 ("00000009" ++ "09" ++ "00000")
        , testFading ( 10, 200 ) 22001 ("0000000a" ++ "00" ++ "00000")
        ]


readSuite200 : Test
readSuite200 =
    describe "read/readOldest should work (200 capacity)"
        [ test "readOldest (0 item)" <| \_ -> Broker.initialize 2 100 |> Broker.readOldest |> Expect.equal Nothing
        , testReadOldestAndRead ( 2, 100 ) 1 1
        , testReadOldestAndRead ( 2, 100 ) 2 2
        , testReadOldestAndRead ( 2, 100 ) 99 99
        , testReadOldestAndRead ( 2, 100 ) 100 100
        , testReadOldestAndRead ( 2, 100 ) 101 101
        , testReadOldestAndRead ( 2, 100 ) 199 199
        , testReadOldestAndRead ( 2, 100 ) 200 200
        , testReadOldestAndRead ( 2, 100 ) 201 201
        , testReadOldestAndRead ( 2, 100 ) 299 299
        , testReadOldestAndRead ( 2, 100 ) 300 300
        , testReadOldestAndRead ( 2, 100 ) 301 201
        , testReadOldestAndRead ( 2, 100 ) 350 250
        , testReadOldestAndRead ( 2, 100 ) 399 299
        , testReadOldestAndRead ( 2, 100 ) 400 300
        , testReadOldestAndRead ( 2, 100 ) 401 201
        ]


testReadOldestAndRead : ( Int, Int ) -> Int -> Int -> Test
testReadOldestAndRead ( numSegments, segmentSize ) appendCount readCount =
    let
        description =
            "readOldest/read "
                ++ fromInt readCount
                ++ " items ("
                ++ fromInt numSegments
                ++ "*"
                ++ fromInt segmentSize
                ++ " capacity, "
                ++ fromInt appendCount
                ++ " items)"
    in
    test description <|
        \_ ->
            Broker.initialize numSegments segmentSize
                |> appendUpto appendCount identity
                |> readAndAssertUpTo readCount (==)


readAndAssertUpTo : Int -> (Int -> a -> Bool) -> Broker a -> Expectation
readAndAssertUpTo count evalItemAtRevIndex broker =
    case Broker.readOldest broker of
        Just ( item, nextOffset ) ->
            if evalItemAtRevIndex count item then
                readAndAssertUpTo_ (count - 1) evalItemAtRevIndex broker nextOffset

            else
                failWithBrokerState broker ("Unexpected item from `readOldest`!: " ++ Debug.toString item)

        otherwise ->
            failWithBrokerState broker ("Unexpected result from `readOldest`!: " ++ Debug.toString otherwise)


readAndAssertUpTo_ : Int -> (Int -> a -> Bool) -> Broker a -> Broker.Offset -> Expectation
readAndAssertUpTo_ count evalItemAtRevIndex broker offset =
    if count <= 0 then
        Broker.read offset broker
            |> Expect.equal Nothing
            |> Expect.onFail "Expected to have consumed all readable items but still getting an item from `read`!"

    else
        case Broker.read offset broker of
            Just ( item, nextOffset ) ->
                if evalItemAtRevIndex count item then
                    readAndAssertUpTo_ (count - 1) evalItemAtRevIndex broker nextOffset

                else
                    failWithBrokerState broker ("Unexpected item at [" ++ Debug.toString count ++ "]!: " ++ Debug.toString item)

            otherwise ->
                failWithBrokerState broker ("Unexpected result at [" ++ Debug.toString count ++ "]!: " ++ Debug.toString otherwise)


failWithBrokerState : Broker a -> String -> Expectation
failWithBrokerState broker message =
    Expect.fail (message ++ "\nBroker State: " ++ Debug.toString broker)


readSuite2000 : Test
readSuite2000 =
    describe "read/readOldest should work (10 * 200 = 2000 capacity)"
        [ testReadOldestAndRead ( 10, 200 ) 2199 2199
        , testReadOldestAndRead ( 10, 200 ) 2200 2200
        , testReadOldestAndRead ( 10, 200 ) 2201 2001
        , testReadOldestAndRead ( 10, 200 ) 4000 2200
        , testReadOldestAndRead ( 10, 200 ) 4001 2001
        ]


getSuite : Test
getSuite =
    describe "get should work"
        [ test "readOldest then get should yield exactly the same item" <|
            \_ ->
                Broker.initialize 2 100
                    |> appendUpto 1 identity
                    |> assertBeforeAfter Broker.readOldest
                        (\broker ( item, offset ) -> Broker.get offset broker |> Expect.equal (Just item))
        , test "readOldest, read, then get should yield the last read item" <|
            \_ ->
                Broker.initialize 2 100
                    |> appendUpto 2 identity
                    |> assertBeforeAfter
                        (\broker -> broker |> Broker.readOldest |> Maybe.andThen (\( _, offset ) -> Broker.read offset broker))
                        (\broker ( item, offset ) -> Broker.get offset broker |> Expect.equal (Just item))
        ]


assertBeforeAfter :
    (Broker a -> Maybe ( a, Broker.Offset ))
    -> (Broker a -> ( a, Broker.Offset ) -> Expectation)
    -> Broker a
    -> Expectation
assertBeforeAfter initialOperation assertAfterOperation broker =
    case initialOperation broker of
        Just itemAndOffset ->
            assertAfterOperation broker itemAndOffset

        Nothing ->
            failWithBrokerState broker "Initial operation did not yield Just value!"


updateSuite : Test
updateSuite =
    describe "update should work"
        [ test "update oldest then get should yield updated oldest item" <|
            \_ ->
                Broker.initialize 2 100
                    |> appendUpto 1 identity
                    |> assertBeforeAfter Broker.readOldest
                        (\broker ( item, offset ) ->
                            broker
                                |> Broker.update offset ((+) 10)
                                |> Broker.get offset
                                |> Expect.equal (Just (item + 10))
                        )
        , test "items in fading segment should not be updated" <|
            \_ ->
                Broker.initialize 2 100
                    |> appendUpto 201 identity
                    |> assertBeforeAfter Broker.readOldest
                        (\broker ( item, offset ) ->
                            broker
                                |> Broker.update offset ((+) 10)
                                |> Broker.get offset
                                |> Expect.equal (Just item)
                        )
        ]


serializerSuite : Test
serializerSuite =
    describe "encode/decoder should work"
        [ testEncodeAndDecode ( 2, 100 ) 0
        , testEncodeAndDecode ( 2, 100 ) 200
        , testEncodeAndDecode ( 2, 100 ) 201
        , testEncodeAndDecode ( 2, 100 ) 301
        , testEncodeAndDecode ( 10, 2000 ) 12345
        ]


testEncodeAndDecode : ( Int, Int ) -> Int -> Test
testEncodeAndDecode ( numSegments, segmentSize ) upTo =
    test (fromInt numSegments ++ "*" ++ fromInt segmentSize ++ " items, appended " ++ fromInt upTo ++ " times") <|
        \_ ->
            let
                broker =
                    Broker.initialize numSegments segmentSize
                        |> appendUpto upTo identity
            in
            broker
                |> Broker.encode Json.Encode.int
                |> Json.Decode.decodeValue (Broker.decoder Json.Decode.int)
                |> Expect.equal (Ok broker)


suite : Test
suite =
    describe "Broker"
        [ initializeSuite
        , appendSuite
        , fadingSuite200
        , fadingSuite2000
        , readSuite200
        , readSuite2000
        , getSuite
        , updateSuite
        , serializerSuite
        ]
