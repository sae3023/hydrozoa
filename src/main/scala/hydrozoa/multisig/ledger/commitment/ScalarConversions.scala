package hydrozoa.multisig.ledger.commitment

import hydrozoa.lib.cardano.scalus.Scalar as ScalusScalar
import scalus.uplc.builtin.ByteString
import supranational.blst.Scalar

object ScalarConversions:
    extension (self: Scalar)
        def asScalusScalar: ScalusScalar =
            ScalusScalar.fromByteStringBigEndianUnsafe(ByteString.fromArray(self.to_bendian()))
