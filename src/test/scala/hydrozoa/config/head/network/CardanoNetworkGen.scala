package hydrozoa.lib.cardano.network

import org.scalacheck.{Arbitrary, Gen}

object CardanoNetworkGen:
    given Arbitrary[CardanoNetwork] = Arbitrary {
        Gen.oneOf(
          CardanoNetwork.Mainnet,
          CardanoNetwork.Preprod,
          CardanoNetwork.Preview
        )
    }
