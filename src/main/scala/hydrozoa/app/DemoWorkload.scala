package hydrozoa.app

import cats.effect.IO
import hydrozoa.config.head.HeadConfig
import hydrozoa.lib.cardano.scalus.ledger.withZeroFees
import hydrozoa.lib.cardano.scalus.txbuilder.DiffHandler.prebalancedLovelaceDiffHandler
import hydrozoa.lib.logging.Logging
import hydrozoa.multisig.MultisigRegimeManager
import hydrozoa.multisig.consensus.*
import hydrozoa.multisig.consensus.peer.HeadPeerWallet
import hydrozoa.multisig.ledger.event.{RequestId, RequestNumber}
import hydrozoa.multisig.ledger.l1.token.CIP67
import io.bullet.borer.Cbor
import scala.concurrent.duration.*
import scalus.cardano.ledger.*
import scalus.cardano.ledger.AuxiliaryData.Metadata
import scalus.cardano.ledger.TransactionOutput.Babbage
import scalus.cardano.txbuilder.TransactionBuilder
import scalus.cardano.txbuilder.TransactionBuilderStep.*
import scalus.uplc.builtin.ByteString

object DemoWorkload {

    private val logger = Logging.loggerIO("hydrozoa.app.DemoWorkload")

    def run(
        connections: MultisigRegimeManager.Connections,
        headConfig: HeadConfig,
        wallet: HeadPeerWallet
    ): IO[Unit] = {

        val evacMap = headConfig.initialEvacuationMap
        val utxos: Map[TransactionInput, TransactionOutput] =
            evacMap.cooked.map((ek, to) =>
                Cbor.decode(ek.byteString.bytes).to[TransactionInput].value -> to
            )

        for {
            _ <- logger.info(s"DemoWorkload: found ${utxos.size} initial L2 UTxOs")

            _ <- utxos.toList.zipWithIndex.traverse_ { case ((txIn, txOut), i) =>
                logger.info(
                  s"  UTxO[$i]: ${txIn.transactionId.toHex}#${txIn.index} => ${txOut.value.coin} lovelace"
                )
            }

            _ <- IO.sleep(5.seconds)

            _ <- logger.info("DemoWorkload: building L2 transaction...")

            (inputTxIn, inputTxOut) = utxos.head
            inputValue = inputTxOut.value
            outputAddress = inputTxOut.address

            halfValue = Value.lovelace(inputValue.coin.value / 2)
            remainderValue = Value.lovelace(inputValue.coin.value - halfValue.coin.value)

            output1 = Babbage(outputAddress, halfValue)
            output2 = Babbage(outputAddress, remainderValue)

            auxiliaryData: Option[AuxiliaryData] = Some(
              Metadata(
                Map(
                  Word64(CIP67.Tags.head)
                      -> Metadatum.List(
                        IndexedSeq(Metadatum.Int(2), Metadatum.Int(2))
                      )
                )
              )
            )

            txResult = TransactionBuilder
                .build(
                  headConfig.network,
                  List(
                    Spend(Utxo(inputTxIn, inputTxOut)),
                    Send(output1),
                    Send(output2),
                    Fee(Coin.zero),
                    ModifyAuxiliaryData(_ => auxiliaryData)
                  )
                )
                .flatMap(
                  _.finalizeContext(
                    protocolParams = headConfig.cardanoProtocolParams.withZeroFees,
                    diffHandler = prebalancedLovelaceDiffHandler,
                    evaluator = headConfig.plutusScriptEvaluatorForTxBuild,
                    validators = Seq.empty
                  )
                )

            _ <- txResult match {
                case Left(err) =>
                    logger.error(s"DemoWorkload: failed to build L2 tx: $err")
                case Right(ctx) =>
                    val txUnsigned = ctx.transaction
                    val txSigned = wallet.signTx(txUnsigned)
                    val l2Payload = ByteString.fromArray(txSigned.toCbor)

                    val body: UserRequestBody.TransactionRequestBody =
                        UserRequestBody.TransactionRequestBody(l2Payload = l2Payload)
                    val header = UserRequestHeader(
                      headId = headConfig.headId,
                      validityStart = RequestValidityStartTimeRaw(
                        java.time.Instant.now().getEpochSecond
                      ),
                      validityEnd = RequestValidityEndTimeRaw(
                        java.time.Instant.now().getEpochSecond + 600
                      ),
                      bodyHash = body.hash
                    )

                    val userRequest = UserRequest.TransactionRequest(
                      header = header,
                      body = body,
                      userVk = wallet.exportVerificationKey
                    )

                    val requestId = RequestId(
                      wallet.getPeerNum,
                      RequestNumber(9000)
                    )

                    val requestWithId = UserRequestWithId(
                      userRequest = userRequest,
                      requestId = requestId
                    )

                    for {
                        _ <- logger.info(
                          s"DemoWorkload: sending L2 tx ${txSigned.id.toHex} to BlockWeaver"
                        )
                        _ <- logger.info(
                          s"  spending: ${inputTxIn.transactionId.toHex}#${inputTxIn.index}"
                        )
                        _ <- logger.info(
                          s"  output1: ${halfValue.coin} lovelace"
                        )
                        _ <- logger.info(
                          s"  output2: ${remainderValue.coin} lovelace"
                        )
                        _ <- connections.blockWeaver ! requestWithId
                        _ <- logger.info("DemoWorkload: L2 transaction submitted to BlockWeaver!")
                    } yield ()
            }
        } yield ()
    }

    private implicit class TraverseOps[A](list: List[A]) {
        def traverse_(f: A => IO[Unit]): IO[Unit] =
            list.foldLeft(IO.unit)((acc, a) => acc *> f(a))
    }
}
