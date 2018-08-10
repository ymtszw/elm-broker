module MainTest exposing (..)

import Expect exposing (Expectation)
import Test exposing (..)
import Broker


suite : Test
suite =
    describe "Broker"
        [ describe "initialize should work"
            [ test "with valid inputs (2, 100)" <| \_ -> Broker.initialize 2 100 |> Broker.capacity |> Expect.equal 200
            , test "with valid inputs (100, 100000)" <| \_ -> Broker.initialize 100 100000 |> Broker.capacity |> Expect.equal 10000000
            , test "with rounded inputs (0, 100)" <| \_ -> Broker.initialize 0 100 |> Broker.capacity |> Expect.equal 200
            , test "with rounded inputs (1, 100)" <| \_ -> Broker.initialize 1 100 |> Broker.capacity |> Expect.equal 200
            , test "with rounded inputs (2, 1)" <| \_ -> Broker.initialize 2 1 |> Broker.capacity |> Expect.equal 200
            , test "with rounded inputs (0, 1)" <| \_ -> Broker.initialize 0 1 |> Broker.capacity |> Expect.equal 200
            , test "with rounded inputs (1, 1)" <| \_ -> Broker.initialize 1 1 |> Broker.capacity |> Expect.equal 200
            , test "with rounded inputs (2, 100001)" <| \_ -> Broker.initialize 2 100001 |> Broker.capacity |> Expect.equal 200000
            ]
        , test "should be empty after initialize" <|
            \_ -> Broker.initialize 2 100 |> Broker.isEmpty |> Expect.true "Expected newly initialized Broker to be empty"
        , describe "append should work indefinitely"
            [ test "up to the capacity of the Broker (200 items)" <|
                \_ ->
                    Broker.initialize 2 100
                        |> appendUpto 200 identity
                        |> Expect.all
                            [ oldestReadableOffsetInString >> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00000001" ++ "00" ++ "00000")
                            ]
            , test "up to second cycle (300 items)" <|
                \_ ->
                    Broker.initialize 2 100
                        |> appendUpto 300 identity
                        |> Expect.all
                            [ oldestReadableOffsetInString >> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00000001" ++ "01" ++ "00000")
                            ]
            , test "for several cycles (10 * 200 = 2000 capacity, 12345 items)" <|
                \_ ->
                    Broker.initialize 10 200
                        |> appendUpto 12345 identity
                        |> Expect.all
                            [ oldestReadableOffsetInString >> Expect.equal (Just ("00000005" ++ "01" ++ "00000"))
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00000006" ++ "01" ++ "00091")
                            ]
            ]
        , describe "oldestReadableOffset should work including fading segments (200 capacity)"
            [ test "not appended" <| \_ -> Broker.initialize 2 100 |> oldestReadableOffsetInString |> Expect.equal Nothing
            , test "append 1 time" <| \_ -> Broker.initialize 2 100 |> appendUpto 1 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 199 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 199 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 200 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 200 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 201 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 201 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 299 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 299 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 300 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 300 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 301 times (1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 301 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 399 times (1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 399 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 400 times (1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 400 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 401 times (2nd segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 401 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
            , test "append 499 times (2nd segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 499 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
            , test "append 500 times (2nd segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 500 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
            , test "append 501 times (3rd segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 501 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "01" ++ "00000"))
            , test "append 601 times (4th segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 601 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000002" ++ "00" ++ "00000"))
            , test "append 701 times (5th segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 701 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000002" ++ "01" ++ "00000"))
            , test "append 801 times (6th segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 801 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000003" ++ "00" ++ "00000"))
            ]
        , describe "oldestReadableOffset should work including fading segments (10 * 200 = 2000 capacity)"
            [ test "append 2200 times" <| \_ -> Broker.initialize 10 200 |> appendUpto 2200 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 2201 times (1st segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2201 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 2300 times (1st segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2300 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 2400 times (1st segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2400 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 2401 times (2nd segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2401 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "02" ++ "00000"))
            , test "append 4000 times (9th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 4000 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "09" ++ "00000"))
            , test "append 4001 times (10th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 4001 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
            , test "append 22000 times (99th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 22000 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000009" ++ "09" ++ "00000"))
            , test "append 22001 times (100th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 22001 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("0000000a" ++ "00" ++ "00000"))
            ]
        , describe "read/readOldest should work (200 capacity)"
            [ test "readOldest (0 item)" <| \_ -> Broker.initialize 2 100 |> Broker.readOldest |> Expect.equal Nothing
            , test "readOldest (1 item, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 1 identity |> readAndAssertUpTo 1 (==)
            , test "readOldest then read (2 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 2 identity |> readAndAssertUpTo 2 (==)
            , test "readOldest then read (99 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 99 identity |> readAndAssertUpTo 99 (==)
            , test "readOldest then read (100 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 100 identity |> readAndAssertUpTo 100 (==)
            , test "readOldest then read (101 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 101 identity |> readAndAssertUpTo 101 (==)
            , test "readOldest then read (199 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 199 identity |> readAndAssertUpTo 199 (==)
            , test "readOldest then read (200 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 200 identity |> readAndAssertUpTo 200 (==)
            , test "readOldest then read (201 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 201 identity |> readAndAssertUpTo 201 (==)
            , test "readOldest then read (299 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 299 identity |> readAndAssertUpTo 299 (==)
            , test "readOldest then read (300 items, no eviction)" <| \_ -> Broker.initialize 2 100 |> appendUpto 300 identity |> readAndAssertUpTo 300 (==)
            , test "readOldest then read (301 items, 1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 301 identity |> readAndAssertUpTo 201 (==)
            , test "readOldest then read (350 items, 1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 350 identity |> readAndAssertUpTo 250 (==)
            , test "readOldest then read (399 items, 1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 399 identity |> readAndAssertUpTo 299 (==)
            , test "readOldest then read (400 items, 1st segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 400 identity |> readAndAssertUpTo 300 (==)
            , test "readOldest then read (401 items, 2nd segment evicted)" <| \_ -> Broker.initialize 2 100 |> appendUpto 401 identity |> readAndAssertUpTo 201 (==)
            ]
        , describe "read/readOldest should work (10 * 200 = 2000 capacity)"
            [ test "readOldest then read (2199 items, no eviction)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2199 identity |> readAndAssertUpTo 2199 (==)
            , test "readOldest then read (2200 items, no eviction)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2200 identity |> readAndAssertUpTo 2200 (==)
            , test "readOldest then read (2201 items, 1st segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 2201 identity |> readAndAssertUpTo 2001 (==)
            , test "readOldest then read (4000 items, 9th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 4000 identity |> readAndAssertUpTo 2200 (==)
            , test "readOldest then read (4001 items, 10th segment evicted)" <| \_ -> Broker.initialize 10 200 |> appendUpto 4001 identity |> readAndAssertUpTo 2001 (==)
            ]
        ]


appendUpto : Int -> (Int -> a) -> Broker.Broker a -> Broker.Broker a
appendUpto count itemGenerator broker =
    if count <= 0 then
        broker
    else
        broker
            |> Broker.append (itemGenerator count)
            |> appendUpto (count - 1) itemGenerator


readAndAssertUpTo : Int -> (Int -> a -> Bool) -> Broker.Broker a -> Expect.Expectation
readAndAssertUpTo count evalItemAtRevIndex broker =
    case Broker.readOldest broker of
        Just ( item, nextOffset ) ->
            if evalItemAtRevIndex count item then
                readAndAssertUpTo_ (count - 1) evalItemAtRevIndex broker nextOffset
            else
                failWithBrokerState broker ("Unexpected item from `readOldest`!: " ++ toString item)

        otherwise ->
            failWithBrokerState broker ("Unexpected result from `readOldest`!: " ++ toString otherwise)


readAndAssertUpTo_ : Int -> (Int -> a -> Bool) -> Broker.Broker a -> Broker.Offset -> Expect.Expectation
readAndAssertUpTo_ count evalItemAtRevIndex broker offset =
    if count <= 0 then
        Broker.read offset broker |> Expect.equal Nothing |> Expect.onFail "Expected to have consumed all readable items but still getting an item from `read`!"
    else
        case Broker.read offset broker of
            Just ( item, nextOffset ) ->
                if evalItemAtRevIndex count item then
                    readAndAssertUpTo_ (count - 1) evalItemAtRevIndex broker nextOffset
                else
                    failWithBrokerState broker ("Unexpected item at [" ++ toString count ++ "]!: " ++ toString item)

            otherwise ->
                failWithBrokerState broker ("Unexpected result at [" ++ toString count ++ "]!: " ++ toString otherwise)


failWithBrokerState : Broker.Broker a -> String -> Expect.Expectation
failWithBrokerState broker message =
    Expect.fail (message ++ "\nBroker State: " ++ toString broker)


oldestReadableOffsetInString : Broker.Broker a -> Maybe String
oldestReadableOffsetInString =
    Broker.oldestReadableOffset >> Maybe.map Broker.offsetToString
