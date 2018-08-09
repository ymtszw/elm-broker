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
            [ test "up to the capacity of the Broker" <|
                \_ ->
                    Broker.initialize 2 100
                        |> appendUpto 200 identity
                        |> Expect.all
                            [ oldestReadableOffsetInString >> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00000001" ++ "00" ++ "00000")
                            ]
            , test "up to second cycle" <|
                \_ ->
                    Broker.initialize 2 100
                        |> appendUpto 300 identity
                        |> Expect.all
                            [ oldestReadableOffsetInString >> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00000001" ++ "01" ++ "00000")
                            ]
            ]
        , describe "oldestReadableOffset should be down to fading segment (200 capacity)"
            [ test "not appended" <| \_ -> Broker.initialize 2 100 |> oldestReadableOffsetInString |> Expect.equal Nothing
            , test "append 1 time" <| \_ -> Broker.initialize 2 100 |> appendUpto 1 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 199 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 199 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 200 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 200 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 201 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 201 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 299 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 299 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "00" ++ "00000"))
            , test "append 300 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 300 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 301 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 301 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 399 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 399 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000000" ++ "01" ++ "00000"))
            , test "append 400 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 400 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
            , test "append 401 times" <| \_ -> Broker.initialize 2 100 |> appendUpto 401 identity |> oldestReadableOffsetInString |> Expect.equal (Just ("00000001" ++ "00" ++ "00000"))
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


oldestReadableOffsetInString : Broker.Broker a -> Maybe String
oldestReadableOffsetInString =
    Broker.oldestReadableOffset >> Maybe.map Broker.offsetToString
