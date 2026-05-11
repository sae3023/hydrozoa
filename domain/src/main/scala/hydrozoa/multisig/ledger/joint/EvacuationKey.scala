package hydrozoa.multisig.ledger.joint

import io.circe.*
import scala.util.Try
import scalus.uplc.builtin.ByteString

given evacuationKeyOrdering: Ordering[EvacuationKey] with {
    override def compare(x: EvacuationKey, y: EvacuationKey): Int =
        summon[Ordering[ByteString]].compare(x.byteString, y.byteString)
}

final case class EvacuationKey private (byteString: ByteString)

object EvacuationKey:
    def apply(bytes: ByteString): Option[EvacuationKey] = Some(new EvacuationKey(bytes))

    given evacuationKeyKeyEncoder: KeyEncoder[EvacuationKey] = {
        KeyEncoder.encodeKeyString.contramap(_.byteString.toHex)
    }

    given evacuationKeyKeyDecoder: KeyDecoder[EvacuationKey] with {
        override def apply(s: String): Option[EvacuationKey] =
            for {
                hex <- KeyDecoder.decodeKeyString(s)
                bytes <- Try(ByteString.fromHex(s)).toOption
                ek <- EvacuationKey(bytes)
            } yield ek
    }
