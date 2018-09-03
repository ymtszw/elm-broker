module HeavyExamples exposing (suite)

import Broker
import Expect
import MainExamples
import Test exposing (..)


suite : Test
suite =
    describe "Broker"
        [ test "append should work indefinitely, above significantly more than the capacity of the Broker (takes around 100s)" <|
            \_ ->
                Broker.initialize 2 100
                    |> MainExamples.appendUpto 123456789 identity
                    |> Expect.all
                        [ MainExamples.oldestReadableOffsetInString >> Expect.equal (Just ("00096b42" ++ "01" ++ "00000"))
                        , Broker.nextOffsetToWrite >> Broker.offsetToString >> Expect.equal ("00096b43" ++ "01" ++ "00059")
                        ]
        ]
