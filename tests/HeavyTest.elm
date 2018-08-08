module HeavyTest exposing (..)

import Expect exposing (Expectation)
import Test exposing (..)
import Broker
import MainTest


suite : Test
suite =
    describe "Broker"
        [ test "append should work indefinitely, above significantly more than the capacity of the Broker (takes around 100s)" <|
            \_ ->
                Broker.initialize 2 100
                    |> MainTest.appendUpto 123456789 "item"
                    |> Expect.all
                        [ MainTest.oldestReadableOffsetInString >> Expect.equal (Just ("00096b42" ++ "01" ++ "00000"))
                        , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00096b43" ++ "01" ++ "00059")
                        ]
        ]
