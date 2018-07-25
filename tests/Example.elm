module Example exposing (..)

import Expect exposing (Expectation)
import Test exposing (..)
import Broker


suite : Test
suite =
    describe "Broker"
        [ describe "initialize should work"
            [ test "with valid inputs (1, 1000)" <| \_ -> Broker.initialize 1 1000 |> Broker.capacity |> Expect.equal 1000
            , test "with valid inputs (1, 10000)" <| \_ -> Broker.initialize 1 10000 |> Broker.capacity |> Expect.equal 10000
            , test "with valid inputs (10, 10000)" <| \_ -> Broker.initialize 10 10000 |> Broker.capacity |> Expect.equal 100000
            , test "with rounded inputs (0, 1000)" <| \_ -> Broker.initialize 0 1000 |> Broker.capacity |> Expect.equal 1000
            , test "with rounded inputs (1, 1)" <| \_ -> Broker.initialize 0 1 |> Broker.capacity |> Expect.equal 1000
            , test "with rounded inputs (0, 1)" <| \_ -> Broker.initialize 0 1 |> Broker.capacity |> Expect.equal 1000
            ]
        ]
