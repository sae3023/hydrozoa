package hydrozoa.multisig.consensus

import cats.effect.{IO, Ref}
import cats.implicits.*
import com.bloxbean.cardano.client.util.HexUtil
import com.suprnation.actor.Actor.{Actor, Receive}
import com.suprnation.actor.ActorRef.ActorRef
import hydrozoa.config.head.initialization.InitialBlock
import hydrozoa.config.head.network.CardanoNetwork
import hydrozoa.config.node.operation.multisig.NodeOperationMultisigConfig
import hydrozoa.lib.cardano.scalus.QuantizedTime.{QuantizedInstant, toEpochQuantizedInstant}
import hydrozoa.lib.logging.Logging
import hydrozoa.multisig.MultisigRegimeManager
import hydrozoa.multisig.backend.cardano.{CardanoBackend, CardanoStreamBackend, UtxoStreamEvent}
import hydrozoa.multisig.consensus.pollresults.PollResults
import hydrozoa.multisig.ledger.block.BlockVersion.Major.increment
import hydrozoa.multisig.ledger.block.{BlockEffects, BlockHeader, BlockVersion}
import hydrozoa.multisig.ledger.l1.tx.*
import scala.collection.immutable.{Seq, TreeMap}
import scala.math.Ordered.orderingToOrdered
import scalus.cardano.ledger.{Block as _, BlockHeader as _, Transaction, TransactionHash, TransactionInput}

/** Streaming-first Cardano liaison. Subscribes to a UTXO event stream and reacts to L1 state
  * changes in real-time rather than polling on a timer.
  */
object CardanoLiaisonStreaming:
    def apply(
        config: Config,
        cardanoBackend: CardanoBackend[IO],
        streamBackend: CardanoStreamBackend,
        pendingConnections: MultisigRegimeManager.PendingConnections |
            CardanoLiaisonStreaming.Connections
    ): IO[CardanoLiaisonStreaming] =
        IO(new CardanoLiaisonStreaming(config, cardanoBackend, streamBackend, pendingConnections) {})

    type Config = CardanoNetwork.Section & InitialBlock.Section &
        NodeOperationMultisigConfig.Section

    final case class Connections(
        blockWeaver: BlockWeaver.Handle
    )

    type EffectId = (BlockVersion.Major, Int)

    object EffectId:
        val initializationEffectId: EffectId = BlockVersion.Major.zero -> 0

    enum TargetState:
        case Active(treasuryUtxoId: TransactionInput)
        case Finalized(finalizationTxHash: TransactionHash)

    type HappyPathEffect = InitializationTx | SettlementTx | FinalizationTx | RolloutTx

    extension (effect: HappyPathEffect)
        def tx: Transaction = effect match
            case e: InitializationTx => e.tx
            case e: SettlementTx     => e.tx
            case e: FinalizationTx   => e.tx
            case e: RolloutTx        => e.tx

    final case class State(
        targetState: TargetState,
        effectInputs: Map[TransactionInput, EffectId],
        happyPathEffects: TreeMap[EffectId, HappyPathEffect],
        fallbackEffects: Map[BlockVersion.Major, FallbackTx]
    )

    object State:
        def initialState(config: Config): State =
            State(
              targetState = TargetState.Active(config.initializationTx.treasuryProduced.utxoId),
              effectInputs = Map.empty,
              happyPathEffects =
                  TreeMap(EffectId.initializationEffectId -> config.initializationTx),
              fallbackEffects = Map(BlockVersion.Major.zero -> config.initialFallbackTx)
            )

        extension (state: State)
            def prettyDump: String = {
                val targetStateStr = state.targetState match {
                    case TargetState.Active(treasuryUtxoId) =>
                        s"Active(treasuryUtxoId=${treasuryUtxoId})"
                    case TargetState.Finalized(finalizationTxHash) =>
                        s"Finalized(txHash=${finalizationTxHash})"
                }

                val effectInputsStr = state.effectInputs
                    .map { case (txIn, effectId) => s"  ${txIn} -> ${effectId}" }
                    .mkString("\n")

                val happyPathEffectsStr = state.happyPathEffects
                    .map { case (effectId, effect) => s"  ${effectId} -> txHash=${effect.tx.id}" }
                    .mkString("\n")

                val fallbackEffectsStr = state.fallbackEffects
                    .map { case (version, fallbackTx) =>
                        s"  ${version} -> txHash=${fallbackTx.tx.id}"
                    }
                    .mkString("\n")

                s"""State(
                   |  targetState: ${targetStateStr}
                   |  effectInputs (${state.effectInputs.size} entries):
                   |${effectInputsStr}
                   |  happyPathEffects (${state.happyPathEffects.size} entries):
                   |${happyPathEffectsStr}
                   |  fallbackEffects (${state.fallbackEffects.size} entries):
                   |${fallbackEffectsStr}
                   |)""".stripMargin
            }

    // ===================================
    // Messages
    // ===================================

    final case class UtxoCreated(utxoId: TransactionInput)
    final case class UtxoSpent(utxoId: TransactionInput)
    case object L1RolledBack
    case object PreStart

    object BlockConfirmed {
        type Major = BlockHeader.Fields.HasBlockVersion & BlockEffects.MultiSigned.Major.Section
        type Final = BlockHeader.Fields.HasBlockVersion & BlockEffects.MultiSigned.Final.Section
    }

    type Request =
        PreStart.type | BlockConfirmed.Major | BlockConfirmed.Final |
            UtxoCreated | UtxoSpent | L1RolledBack.type
    type Handle = ActorRef[IO, Request]

end CardanoLiaisonStreaming

trait CardanoLiaisonStreaming(
    config: CardanoLiaisonStreaming.Config,
    cardanoBackend: CardanoBackend[IO],
    streamBackend: CardanoStreamBackend,
    pendingConnections: MultisigRegimeManager.PendingConnections |
        CardanoLiaisonStreaming.Connections,
) extends Actor[IO, CardanoLiaisonStreaming.Request]:
    import CardanoLiaisonStreaming.*

    private val logger = Logging.logger("CardanoLiaisonStreaming")
    private val loggerIO = Logging.loggerIO("CardanoLiaisonStreaming")

    private val connections = Ref.unsafe[IO, Option[Connections]](None)
    private val stateRef = Ref.unsafe[IO, State](State.initialState(config))
    private val trackedUtxos = Ref.unsafe[IO, Set[TransactionInput]](Set.empty)

    private def getConnections: IO[Connections] = this.connections.get.flatMap(
      _.fold(
        IO.raiseError(RuntimeException("CardanoLiaisonStreaming missing connections."))
      )(IO.pure)
    )

    private def initializeConnections: IO[Unit] = pendingConnections match {
        case x: MultisigRegimeManager.PendingConnections =>
            x.get.flatMap(c => connections.set(Some(Connections(blockWeaver = c.blockWeaver))))
        case x: CardanoLiaisonStreaming.Connections => connections.set(Some(x))
    }

    override def preStart: IO[Unit] = context.self ! PreStart

    override def receive: Receive[IO, Request] = PartialFunction.fromFunction(receiveTotal)

    private def receiveTotal(req: Request): IO[Unit] = req match {
        case PreStart              => preStartLocal
        case block: BlockConfirmed.Major => handleMajorBlockL1Effects(block) >> runEffects
        case block: BlockConfirmed.Final => handleFinalBlockL1Effects(block) >> runEffects
        case event: UtxoCreated    => handleUtxoCreated(event)
        case event: UtxoSpent      => handleUtxoSpent(event)
        case L1RolledBack          => handleL1RolledBack
    }

    private def preStartLocal: IO[Unit] =
        for {
            _ <- initializeConnections
            _ <- startStreaming
        } yield ()

    private def startStreaming: IO[Unit] =
        val address = config.initializationTx.treasuryProduced.address
        val self = context.self
        streamBackend
            .subscribeUtxos(address)
            .evalMap {
                case UtxoStreamEvent.Created(utxoId) => self ! UtxoCreated(utxoId)
                case UtxoStreamEvent.Spent(utxoId)   => self ! UtxoSpent(utxoId)
                case UtxoStreamEvent.RolledBack      => self ! L1RolledBack
            }
            .compile
            .drain
            .handleErrorWith(e =>
                loggerIO.error(s"UTXO stream failed: ${e.getMessage}")
            )
            .start
            .void

    // ===================================
    // Stream event handlers
    // ===================================

    private def handleUtxoCreated(event: UtxoCreated): IO[Unit] =
        trackedUtxos.update(_ + event.utxoId) >> notifyAndRunEffects

    private def handleUtxoSpent(event: UtxoSpent): IO[Unit] =
        trackedUtxos.update(_ - event.utxoId) >> notifyAndRunEffects

    private def handleL1RolledBack: IO[Unit] =
        for {
            _ <- loggerIO.warn("L1 rollback detected, re-polling full state")
            resp <- cardanoBackend.utxosAt(config.initializationTx.treasuryProduced.address)
            _ <- resp match {
                case Left(err) =>
                    loggerIO.error(s"Failed to re-poll after rollback: ${err}")
                case Right(utxos) =>
                    trackedUtxos.set(utxos.keySet) >> notifyAndRunEffects
            }
        } yield ()

    private def notifyAndRunEffects: IO[Unit] =
        for {
            utxoIds <- trackedUtxos.get
            conn <- getConnections
            _ <- conn.blockWeaver ! PollResults(utxoIds)
            _ <- runEffectsWithUtxos(utxoIds)
        } yield ()

    // ===================================
    // Block effect handlers
    // ===================================

    protected[consensus] def handleMajorBlockL1Effects(block: BlockConfirmed.Major): IO[Unit] =
        for {
            _ <- IO.whenA(block.blockVersion.major != block.settlementTx.majorVersionProduced) {
                val msg =
                    s"Block major version (${block.blockVersion.major}) doesn't match" +
                        s" settlement tx major version produced (${block.settlementTx.majorVersionProduced})"
                loggerIO.error(msg) >> IO.raiseError(RuntimeException(msg))
            }
            _ <- loggerIO.info(s"handleMajorBlockL1Effects for block ${block.blockVersion}")
            _ <- stateRef.update(s => {
                val (blockEffectInputs, blockEffects) =
                    mkHappyPathEffectInputsAndEffects(block.settlementTx, block.rolloutTxs)
                State(
                  targetState = TargetState.Active(block.settlementTx.treasuryProduced.utxoId),
                  effectInputs = s.effectInputs ++ blockEffectInputs,
                  happyPathEffects = s.happyPathEffects ++ blockEffects,
                  fallbackEffects =
                      s.fallbackEffects + (block.blockVersion.major -> block.fallbackTx)
                )
            })
        } yield ()

    protected[consensus] def handleFinalBlockL1Effects(block: BlockConfirmed.Final): IO[Unit] =
        for {
            _ <- loggerIO.info(s"handleFinalBlockL1Effects for block ${block.blockVersion}")
            _ <- stateRef.update(s => {
                val (blockEffectInputs, blockEffects) =
                    mkHappyPathEffectInputsAndEffects(block.finalizationTx, block.rolloutTxs)
                s.copy(
                  targetState = TargetState.Finalized(block.finalizationTx.tx.id),
                  effectInputs = s.effectInputs ++ blockEffectInputs,
                  happyPathEffects = s.happyPathEffects ++ blockEffects
                )
            })
        } yield ()

    // ===================================
    // Effect execution
    // ===================================

    private def runEffects: IO[Unit] =
        trackedUtxos.get.flatMap(runEffectsWithUtxos)

    private def runEffectsWithUtxos(utxoIds: Set[TransactionInput]): IO[Unit] = for {
        state <- stateRef.get
        currentTime <- IO.realTime.map(_.toEpochQuantizedInstant(config.slotConfig))

        _ <- loggerIO.trace(s"current time is $currentTime")
        _ <- loggerIO.trace(s"utxoIds are $utxoIds")
        _ <- loggerIO.trace(s"state is ${state.prettyDump}")

        dueActions: Seq[DirectAction] <- mkDirectActions(state, utxoIds, currentTime).fold(
          e => loggerIO.error(s"Critical error: ${e.msg}") >> IO.raiseError(RuntimeException(e.msg)),
          IO.pure
        )

        actionsToSubmit <-
            if dueActions.nonEmpty then IO.pure(dueActions)
            else mkFallbackOrInitActions(state, utxoIds, currentTime)

        _ <- IO.whenA(actionsToSubmit.nonEmpty)(
          loggerIO.info(
            "Liaison's actions:" + actionsToSubmit.map(a => s"\n\t- ${a.msg}").mkString
          )
        )

        submitRet <-
            if actionsToSubmit.nonEmpty then
                IO.traverse(actionsToSubmit.flatMap(actionTxs).toList)(tx =>
                    for {
                        _ <- loggerIO.trace(
                          s"Submitting tx hash: ${tx.id} cbor: ${HexUtil.encodeHexString(tx.toCbor)}"
                        )
                        ret <- cardanoBackend.submitTx(tx)
                    } yield tx -> ret
                )
            else IO.pure(List.empty)

        submissionErrors = submitRet.filter(_._2.isLeft)
        _ <- IO.whenA(submissionErrors.nonEmpty)(
          loggerIO.debug(
            "Submission errors (generally ignored):" + submissionErrors
                .map(a => s"\n\t- ${a._2.left}, cbor=${HexUtil.encodeHexString(a._1.toCbor)}")
                .mkString
          )
        )
    } yield ()

    private def mkFallbackOrInitActions(
        state: State,
        utxoIds: Set[TransactionInput],
        currentTime: QuantizedInstant
    ): IO[Seq[Action]] = {
        val lastFallback: Option[Transaction] = for {
            maxKey <- state.fallbackEffects.keySet.maxOption
            fallbackTx = state.fallbackEffects(maxKey)
            if utxoIds.contains(
              fallbackTx.treasurySpent.utxoId
            ) && fallbackTx.fallbackTxStartTime.convert <= currentTime
        } yield fallbackTx.tx

        lastFallback match {
            case Some(fallback) =>
                IO.pure(Seq(Action.FallbackToRuleBased(fallback)))
            case None =>
                lazy val initAction =
                    if currentTime < config.initializationTx.initializationTxEndTime.convert
                    then
                        Seq(Action.InitializeHead(state.happyPathEffects.values.map(_.tx).toSeq))
                    else Seq.empty

                state.targetState match {
                    case TargetState.Active(targetTreasuryUtxoId) =>
                        if utxoIds.contains(targetTreasuryUtxoId) then
                            loggerIO.trace(s"target $targetTreasuryUtxoId found, do nothing") >>
                                IO.pure(Seq.empty)
                        else IO.pure(initAction)

                    case TargetState.Finalized(finalizationTxHash) =>
                        cardanoBackend.isTxKnown(finalizationTxHash).flatMap {
                            case Left(err) =>
                                loggerIO.error(
                                  s"error when getting finalization tx info: ${err}"
                                ) >> IO.pure(Seq.empty)
                            case Right(true) =>
                                loggerIO.trace("finalization tx is known, do nothing") >>
                                    IO.pure(Seq.empty)
                            case Right(false) => IO.pure(initAction)
                        }
                }
        }
    }

    // ===================================
    // Actions
    // ===================================

    sealed trait Action
    sealed trait DirectAction extends Action

    object Action {
        final case class FallbackToRuleBased(tx: Transaction) extends DirectAction
        final case class PushForwardMultisig(txs: Seq[Transaction]) extends DirectAction
        final case class Rollout(txs: Seq[Transaction]) extends DirectAction
        case object SilencePeriodNoop extends DirectAction
        final case class InitializeHead(txs: Seq[Transaction]) extends Action
    }

    private def actionTxs(action: Action): Seq[Transaction] = action match {
        case Action.FallbackToRuleBased(tx)  => Seq(tx)
        case Action.PushForwardMultisig(txs) => txs
        case Action.Rollout(txs)             => txs
        case Action.SilencePeriodNoop        => Seq.empty
        case Action.InitializeHead(txs)      => txs
    }

    extension (action: Action)
        private def msg: String = action match {
            case Action.FallbackToRuleBased(tx)  => s"FallbackToRuleBased (${tx.id})"
            case Action.PushForwardMultisig(txs) => s"PushForwardMultisig (${txs.map(_.id)}"
            case Action.Rollout(txs)             => s"Rollout (${txs.map(_.id)}"
            case Action.SilencePeriodNoop        => "SilencePeriodNoop"
            case Action.InitializeHead(txs)      => s"InitializeHead (${txs.map(_.id)}"
        }

    // ===================================
    // Effect logic
    // ===================================

    private def mkHappyPathEffectInputsAndEffects(
        majorTx: SettlementTx | FinalizationTx,
        rollouts: List[RolloutTx]
    ): (Seq[(TransactionInput, EffectId)], Seq[(EffectId, HappyPathEffect)]) = {
        val effects: List[(TransactionInput, HappyPathEffect)] =
            List(majorTx.treasurySpent.utxoId -> majorTx) ++
                rollouts.map(r => r.rolloutSpent.utxo.input -> r)
        indexWithEffectId(effects, majorTx.majorVersionProduced).unzip
    }

    private def indexWithEffectId(
        effects: List[(TransactionInput, HappyPathEffect)],
        versionMajor: BlockVersion.Major
    ): List[((TransactionInput, EffectId), (EffectId, HappyPathEffect))] =
        effects.zipWithIndex.map { (utxoIdAndEffect, index) =>
            val effectId = versionMajor -> index
            (utxoIdAndEffect._1 -> effectId) -> (effectId -> utxoIdAndEffect._2)
        }

    private def mkDirectActions(
        state: State,
        utxosFound: Set[TransactionInput],
        currentTime: QuantizedInstant
    ): Either[EffectError, Seq[DirectAction]] =
        utxosFound
            .flatMap(state.effectInputs.get)
            .toSeq
            .sorted
            .map(mkDirectAction(state, currentTime))
            .sequence

    private def mkDirectAction(state: State, currentTime: QuantizedInstant)(
        effectId: EffectId
    ): Either[EffectError, DirectAction] = {
        import Action.*
        import EffectError.*

        effectId match {
            case backboneEffectId @ (versionMajor, 0) =>
                val happyPathEffect = state.happyPathEffects(backboneEffectId)
                val mbCompetingFallbackEffect = state.fallbackEffects.get(versionMajor.decrement)

                mbCompetingFallbackEffect match {
                    case Some(fallback) =>
                        for {
                            happyPathTxTtl: QuantizedInstant <- happyPathEffect match {
                                case tx: SettlementTx =>
                                    Right(tx.settlementTxEndTime.convert)
                                case tx: FinalizationTx =>
                                    Right(tx.finalizationTxEndTime.convert)
                                case _: InitializationTx =>
                                    Left(UnexpectedInitializationEffect(backboneEffectId))
                                case _: RolloutTx =>
                                    Left(UnexpectedRolloutEffect(backboneEffectId))
                            }
                            fallbackValidityStart = fallback.fallbackTxStartTime
                            ret <-
                                if currentTime < happyPathTxTtl then
                                    val effectTxs =
                                        state.happyPathEffects.rangeFrom(backboneEffectId).toSeq
                                            .map(_._2)
                                    Right(PushForwardMultisig(effectTxs.map(_.tx)))
                                else if currentTime < fallbackValidityStart then
                                    Right(SilencePeriodNoop)
                                else if currentTime >= fallbackValidityStart then
                                    Right(FallbackToRuleBased(fallback.tx))
                                else
                                    Left(
                                      WrongValidityRange(
                                        currentTime,
                                        happyPathTxTtl,
                                        fallbackValidityStart
                                      )
                                    )
                        } yield ret

                    case None =>
                        Left(MissingCompetingFallback(backboneEffectId))
                }

            case rolloutTx @ (versionMajor, _) =>
                val nextBackboneTx = versionMajor.increment -> 0
                val effectTxs =
                    state.happyPathEffects.range(rolloutTx, nextBackboneTx).toSeq.map(_._2)
                Right(Rollout(effectTxs.map(_.tx)))
        }
    }

    private enum EffectError extends Throwable:
        case UnexpectedRolloutEffect(effectId: EffectId)
        case UnexpectedInitializationEffect(effectId: EffectId)
        case MissingCompetingFallback(effectId: EffectId)
        case WrongValidityRange(
            currentTime: QuantizedInstant,
            happyPathTtl: QuantizedInstant,
            fallbackValidityStart: QuantizedInstant
        )

    import EffectError.*

    extension (self: EffectError)
        private def msg: String = self match {
            case UnexpectedRolloutEffect(effectId) =>
                s"Unexpected rollout effect with effectId = $effectId, check the integrity of effects."
            case UnexpectedInitializationEffect(effectId) =>
                s"Unexpected initialization effect with effectId = $effectId, check the integrity of effects and the initialization tx."
            case MissingCompetingFallback(effectId) =>
                s"Impossible: a settlement/finalization effect ($effectId) without a competing fallback tx."
            case WrongValidityRange(currentTime, happyPathTtl, fallbackValidityStart) =>
                s"Validity range invariant is not hold: current time: $currentTime," +
                    s" happy path tx TTL: $happyPathTtl" +
                    s" fallback validity start: $fallbackValidityStart"
        }

end CardanoLiaisonStreaming
