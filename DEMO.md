# Hydrozoa Demo: Auction Test Bench

This demo runs two programs: Hydrozoa (handling L2 transactions) and an auction test bench (generating load via the scalus auction validator).

## Prerequisites

- Nix shell active in both terminals
- Both repos checked out as siblings: `hydrozoa/` and `scalus/`

## 1. Start Hydrozoa

Hydrozoa runs locally with an in-memory emulator backend (no real Cardano node needed).

```bash
cd hydrozoa

# Ensure .env file exists with required variables:
# BLOCKFROST_API_KEY=dummy
# CARDANO_VERIFICATION_KEY=<64 hex chars>
# CARDANO_SIGNING_KEY=<64 hex chars>
# EQUITY=2000000
# ADMIN_USERNAME=admin
# ADMIN_PASSWORD=welcome

sbtn "runMain hydrozoa.app.Main"
```

Wait for the log line:
```
Starting HTTP server...
```

Hydrozoa is now listening on `http://localhost:8080`.

Verify with:
```bash
curl http://localhost:8080/health
# {"status":"ok"}

curl http://localhost:8080/api/head-info
# Returns headId, headAddress, timing config
```

## 2. Run the Auction Test Bench

In a second terminal:

```bash
cd scalus

sbtn "scalusExamplesJVM/runMain scalus.examples.auction.AuctionBench"
```

Optional arguments:
```bash
# Custom URL and number of bid rounds
sbtn "scalusExamplesJVM/runMain scalus.examples.auction.AuctionBench http://localhost:8080 5"
```

The bench will:
1. Connect to Hydrozoa and fetch head-info
2. Start an auction (mint NFT + create auction UTxO)
3. Run N bid rounds with two competing bidders
4. End the auction (transfer NFT to winner, funds to seller)
5. Print throughput metrics

## What to expect

```
Auction Bench — connecting to http://localhost:8080
  headId: 01349900acc00fa69e4e0f0c4bfab3bd...
  headAddress: addr_test1...

--- Starting auction ---
  start tx: a1b2c3...

--- Running 3 bid rounds ---
  round 1: bidderA bids 3000000 lovelace
    tx: d4e5f6...
  round 1: bidderB bids 4000000 lovelace
    tx: 789abc...
  ...

--- Ending auction ---
  end tx: def012...

--- Results ---
  transactions: 8
  elapsed: 1234ms
  throughput: 6.48 tx/s
```

## Architecture

```
┌─────────────────────┐         HTTP          ┌──────────────────┐
│  Auction Bench      │ ──────────────────── │  Hydrozoa        │
│  (scalus repo)      │  POST /api/l2/submit  │  (emulator mode) │
│                     │ ◄──────────────────── │                  │
│  Builds Cardano txs │  { requestId: ... }   │  Validates L2 txs│
│  via scalus TxBuilder│                       │  Sequences blocks│
└─────────────────────┘                       └──────────────────┘
```
