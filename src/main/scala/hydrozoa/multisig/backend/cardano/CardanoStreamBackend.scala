package hydrozoa.multisig.backend.cardano

import cats.effect.IO
import fs2.Stream
import scalus.cardano.address.ShelleyAddress
import scalus.cardano.ledger.TransactionInput

/** Push-based streaming interface for Cardano L1 UTXO events.
  *
  * When provided to the CardanoLiaison, it replaces timer-based polling with real-time event
  * delivery. The liaison maintains a local UTXO set incrementally from stream events and reacts
  * immediately to L1 state changes.
  *
  * Implement this trait by wrapping a scalus-node Fs2BlockchainStreamProvider's
  * `subscribeUtxoQuery` method once the streaming library is published.
  */
trait CardanoStreamBackend:

    /** Stream of UTXO lifecycle events at the given address.
      *
      * The stream must:
      *   - Emit [[UtxoStreamEvent.Created]] for each existing UTXO at subscription time (seeding)
      *   - Emit [[UtxoStreamEvent.Created]] / [[UtxoStreamEvent.Spent]] as blocks confirm
      *   - Emit [[UtxoStreamEvent.RolledBack]] on chain reorganization
      *   - Never terminate under normal operation (long-lived subscription)
      */
    def subscribeUtxos(address: ShelleyAddress): Stream[IO, UtxoStreamEvent]

/** Events emitted by the UTXO stream, modeled after scalus-node's UtxoEvent. */
enum UtxoStreamEvent:
    case Created(utxoId: TransactionInput)
    case Spent(utxoId: TransactionInput)
    case RolledBack
