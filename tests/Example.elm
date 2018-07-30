module Example exposing (..)

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
                    List.repeat 200 "item"
                        |> List.foldl (\item broker -> Broker.append item broker) (Broker.initialize 2 100)
                        |> Expect.all
                            [ Broker.capacity >> Expect.equal 200
                            , Broker.isEmpty >> Expect.false "Expected Broker is not empty"
                            , Broker.oldestReadableOffset >> Maybe.map Broker.offsetToString >> Expect.equal (Just "000000000000000")
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal "000000010000000"
                            ]
            , test "above significantly more than the capacity of the Broker" <|
                \_ ->
                    List.repeat 12345678 "item"
                        |> List.foldl (\item broker -> Broker.append item broker) (Broker.initialize 2 100)
                        |> Expect.all
                            [ Broker.capacity >> Expect.equal 200
                            , Broker.isEmpty >> Expect.false "Expected Broker is not empty"
                            , Broker.oldestReadableOffset >> Maybe.map Broker.offsetToString >> Expect.equal (Just "0000f11f000004e")
                            , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal "0000f120000004e"
                            ]
            ]
        ]
