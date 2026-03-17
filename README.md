# 🏦 Real World Asset (RWA) Tokenization System (Python)

> ⚠️ **SANDBOX / EDUCATIONAL USE ONLY — NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not legally reviewed, and must not be used to tokenize, manage, or transfer real assets or investor capital.** See the [Production Warning](#-production-warning) section for full details.

---

## Table of Contents

- [Overview](#-overview)
- [What is RWA Tokenization?](#-what-is-rwa-tokenization)
- [Architecture](#-architecture)
- [Core Services](#-core-services)
- [Key Features & Design Patterns](#-key-features--design-patterns)
- [Database Schema](#-database-schema)
- [State Machines](#-state-machines)
- [Real-World Example: BlackRock BUIDL](#-real-world-example-blackrock-buidl)
- [Running in a Sandbox Environment](#-running-in-a-sandbox-environment)
- [Project Structure](#-project-structure)
- [Production Warning](#-production-warning)
- [License](#-license)

---

## 📖 Overview

The **RWA Tokenization System** is a Python-based reference implementation that models the full lifecycle of tokenizing a real-world asset — from initial registration through legal structuring, investor onboarding, token minting, daily yield distribution, redemption, and on-chain/off-chain reconciliation.

The system is modeled closely on how institutional-grade tokenization platforms such as **BlackRock BUIDL**, **Franklin OnChain U.S. Government Money Fund**, and **Ondo Finance** operate in practice. It demonstrates how traditional financial infrastructure (custody, KYC, wire transfers, legal entities) integrates with blockchain technology (ERC-1400 security tokens, smart contract transfer restrictions, on-chain confirmation tracking).

| Component | File | Responsibility |
|---|---|---|
| Core Services | `rwa_tokenization.py` | All service classes — registration through reconciliation |
| Database Schema & Demo | `rwa_tokenization.sql` | PostgreSQL schema, indexes, and a full BUIDL-style walkthrough |

---

## 🌍 What is RWA Tokenization?

**Real World Asset tokenization** is the process of representing ownership of a physical or traditional financial asset as a digital token on a blockchain. Examples include:

- A **US Treasury bill fund** where each token = $1.00 of NAV (BlackRock BUIDL)
- A **commercial real estate property** held in an SPV, with tokens representing equity shares
- A **private equity fund** where tokens represent LP interests
- A **commodity** (gold, oil) with tokens backed by physically-held inventory

Unlike permissionless DeFi tokens, RWA tokens are **regulated securities**. Every holder must pass KYC/AML, every transfer must be between whitelisted wallets, and the legal structure (SPV, Trust, LLC) is what gives the token actual enforceable ownership rights over the underlying asset. Without the legal wrapper, the token is just a receipt — not a legal claim.

---

## 🏗 Architecture

```
                    ┌────────────────────────────────────────────────────┐
                    │                   RWA LIFECYCLE                    │
                    └────────────────────────────────────────────────────┘

  Off-Chain                                              On-Chain
  ─────────────────────────────────────────────────────────────────────

  1. RWARegistry              Register asset
     (register_asset)  ──────────────────────────────► PostgreSQL
                                                        + Outbox Event

  2. LegalWrapperService      Create SPV / Trust / Fund
     (create_legal_wrapper) ─────────────────────────► Legal entity
                                                        + Token supply init

  3. KYCComplianceService     Sanctions + KYC check
     (onboard_investor) ──── External APIs ──────────► Whitelist wallet
                              (OFAC, Securitize)        + Outbox Event

  4. TokenMintingService      Investor wires fiat
     (mint_tokens) ──────────────────────────────────► Lock supply
                                                        + Signing Queue
                                                              │
                                                              ▼
                                                        ERC-1400 mint()
                                                        on Ethereum

  5. NAVCalculationEngine     Daily yield (T-bill APY)
     (calculate_and_           total_value += yield
      distribute_yield) ──────────────────────────────► Rebase tokens
                                                        + Outbox Event

  6. TokenRedemptionService   Investor returns tokens
     (request_redemption) ───────────────────────────► Burn on-chain
     (settle_redemption)                                Wire fiat back

  7. RWAReconciliationEngine  Daily verification
     (reconcile_asset) ──── Blockchain node ─────────► Mismatch alert
                         └── Custodian API              Halt if broken
```

---

## ⚙️ Core Services

### `RWARegistry`
The entry point for any new tokenized asset. Validates that the custodian is licensed and approved, writes the asset record and an outbox event atomically, and begins the asset at `PENDING_LEGAL` status. Idempotency is enforced at the registration key level, preventing the same asset from ever being registered twice.

### `LegalWrapperService`
Creates the legal entity (SPV, Delaware Trust, LLC, or regulated Fund) that holds the real-world asset. This is the most critical layer in RWA infrastructure — it establishes the enforceable legal claim that backs each token. Calculates token economics (`price_per_token = total_value / token_supply`), initializes the token supply tracker, and advances the asset state machine to `PENDING_AUDIT`.

### `KYCComplianceService`
Handles full investor onboarding with a three-phase approach. First, it runs an OFAC/EU/UN sanctions screening. Then it calls the KYC provider (e.g., Securitize) for identity and accreditation verification. Both external calls happen **outside** the database transaction to avoid holding row locks during slow API calls. Finally, it atomically writes the compliance record and whitelists the investor's wallet address. Also exposes `can_transfer()` — called before every single token transfer — and `revoke_whitelist()` for expired or exited investors.

### `TokenMintingService`
Triggered when an investor's fiat wire is received. Uses pessimistic locking (`SELECT ... FOR UPDATE`) on the token supply row to prevent overselling. Atomically reserves tokens, creates the mint record, and writes an outbox event in a single transaction. The signing queue message is sent outside the transaction. On-chain confirmation is handled by `confirm_mint()`, which settles the ledger.

### `NAVCalculationEngine`
Runs daily to calculate and distribute yield from the underlying asset (e.g., T-bill interest). Computes `daily_yield = total_value × (annual_rate / 365)`, updates the fund's total value, and emits an outbox event that triggers a token rebase on-chain — the mechanism by which investors receive yield as additional tokens rather than as a price increase.

### `TokenRedemptionService`
The inverse of minting. Manages the full redemption lifecycle: verifying the investor's compliance status, locking their token balance, publishing a burn transaction to the signing queue, and finally settling by releasing supply counters and triggering a fiat wire transfer. The most operationally complex flow because it coordinates three separate systems — blockchain, compliance, and traditional banking — that must all succeed or roll back together.

### `RWAReconciliationEngine`
Daily audit engine that cross-checks four sources of truth: the internal ledger vs. on-chain token supply, the fund NAV vs. the custodian's reported assets under management, and a scan for tokens held by non-whitelisted wallets. Any discrepancy immediately triggers a critical alert and halts all new minting and redemptions until the mismatch is resolved.

### `RWAOutboxPublisher`
Async background poller that delivers `outbox_events` to Kafka. Uses `FOR UPDATE SKIP LOCKED` for safe multi-instance operation. Marks each event `published_at` only after successful Kafka delivery, ensuring exactly-once delivery semantics even across crashes and restarts.

---

## ✨ Key Features & Design Patterns

### ✅ Full Asset Lifecycle State Machine
Assets progress through a strict sequence: `PENDING_LEGAL → PENDING_AUDIT → APPROVED → TOKENIZED → REDEEMED`. State guards at each transition prevent operations from running out of order — you cannot mint tokens for an asset that hasn't cleared legal review.

### ✅ Permissioned Transfers (ERC-1400 Pattern)
Unlike standard ERC-20 tokens, RWA tokens enforce transfer restrictions at the smart contract level. The `can_transfer()` check gates every token movement — both sender and receiver must appear in `whitelisted_wallets`. This mirrors how ERC-1400 security token standards work in production systems like Securitize and Tokeny.

### ✅ KYC Expiry with Mandatory Re-verification
Every investor compliance record carries an `expires_at` timestamp. When KYC expires, `revoke_whitelist()` removes the wallet from the whitelist and blocks all future transfers. This models the annual re-verification requirement imposed by regulated platforms handling securities.

### ✅ Idempotency at Every Layer
Each service method checks for an existing `idempotency_key` before any side effect. This covers: asset registration, investor onboarding, token minting, NAV calculations, and redemptions. Re-submitted requests return the original result with no duplicate operations.

### ✅ Pessimistic Locking on Token Supply
The `rwa_token_supply` row is locked with `SELECT ... FOR UPDATE` before every mint, preventing two concurrent investments from both seeing sufficient supply and both succeeding — the exact double-mint race condition that has caused real losses on production token platforms.

### ✅ Transactional Outbox Pattern (All Services)
Every service writes its outbox event in the **same database transaction** as the business record. This eliminates the dual-write problem across all six services. The `RWAOutboxPublisher` delivers events to Kafka only after they are safely committed to Postgres, so downstream services (investor dashboards, compliance systems, on-chain oracles) never miss an event even if the application crashes.

### ✅ Atomic Ledger Settlement
Token supply updates (`minted_supply += amount`) and record creation are always co-located in the same transaction. During redemption, the supply release and settlement status update are committed together or not at all — the ledger never shows tokens as both outstanding and redeemed.

### ✅ Reconciliation as a Hard Stop
The `RWAReconciliationEngine` treats any discrepancy between internal state and on-chain reality as an emergency, not a warning. Minting and redemptions are halted immediately, ensuring financial integrity is never traded off against operational continuity.

### ✅ Decimal Precision for Financial Arithmetic
All token amounts and monetary values use Python's `Decimal` type rather than floats, preventing IEEE 754 rounding errors from corrupting financial calculations — a mandatory requirement in any system handling currency.

---

## 🗄 Database Schema

### `rwa_assets`
The root record for every tokenized asset.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `type` | VARCHAR(20) | `real_estate`, `treasury_bond`, `private_equity`, `commodity`, `fund` |
| `total_value` | DECIMAL(38,18) | Current AUM / asset value (updated daily on yield accrual) |
| `custodian` | VARCHAR(256) | Licensed custodian holding the underlying asset |
| `status` | VARCHAR(20) | Asset lifecycle state |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate registration |

### `legal_wrappers`
The legal entity bridging off-chain ownership to on-chain tokens.

| Column | Type | Description |
|---|---|---|
| `structure_type` | VARCHAR(20) | `SPV`, `TRUST`, `LLC`, `FUND` |
| `token_supply` | DECIMAL(38,18) | Maximum number of tokens that can be minted |
| `price_per_token` | DECIMAL(38,18) | `total_value / token_supply` at issuance |

### `investor_compliance`
KYC/AML record per investor, tied to a specific wallet address.

| Column | Type | Description |
|---|---|---|
| `tier` | VARCHAR(20) | `retail`, `accredited`, `qualified`, `institutional` |
| `kyc_reference` | VARCHAR(256) | Reference ID from KYC provider (e.g., Securitize) |
| `expires_at` | TIMESTAMP | When KYC expires — re-verification required |
| `status` | VARCHAR(20) | `pending`, `approved`, `rejected`, `expired` |

### `whitelisted_wallets`
The gate for all token transfers. Every row is a wallet address that may legally hold and transfer tokens.

| Column | Type | Description |
|---|---|---|
| `wallet_address` | VARCHAR(256) UNIQUE | On-chain Ethereum address |
| `tier` | VARCHAR(20) | Investor classification |

### `token_mints`
A record for every token issuance event.

| Column | Type | Description |
|---|---|---|
| `token_amount` | DECIMAL(38,18) | Number of tokens issued |
| `fiat_received` | DECIMAL(38,18) | USD amount wired by investor |
| `confirmed_at` | TIMESTAMP | NULL = in-flight; NOT NULL = confirmed on-chain |

### `outbox_events`
Reliable event delivery buffer — shared across all services.

| Column | Type | Description |
|---|---|---|
| `event_type` | VARCHAR(64) | e.g. `rwa.registered`, `token.mint_confirmed`, `nav.yield_calculated` |
| `payload` | JSONB | Full event data |
| `published_at` | TIMESTAMP | NULL = pending Kafka delivery |

---

## 🔁 State Machines

### Asset Status
```
                      ┌──────────────────┐
  register_asset ────►│  PENDING_LEGAL   │
                      └────────┬─────────┘
                               │  create_legal_wrapper()
                               ▼
                      ┌──────────────────┐
                      │  PENDING_AUDIT   │
                      └────────┬─────────┘
                               │  audit passes
                               ▼
                      ┌──────────────────┐
                      │    APPROVED      │
                      └────────┬─────────┘
                               │  first mint confirmed
                               ▼
                      ┌──────────────────┐
                      │   TOKENIZED      │
                      └────────┬─────────┘
                               │  all tokens redeemed
                               ▼
                      ┌──────────────────┐
                      │    REDEEMED      │
                      └──────────────────┘
```

### Token Mint Status
```
  mint_tokens() ────► PENDING ──► APPROVED ──► SIGNED ──► BROADCAST
                                                                │
                                                    ┌───────────┴──────────┐
                                                    ▼                      ▼
                                               CONFIRMED               FAILED
```

### Redemption Status
```
  request_redemption() ──► PENDING ──► APPROVED ──► BURNING ──► BURNED ──► SETTLED
                                                                              │
                                                                        (fiat wired)
```

---

## 🏢 Real-World Example: BlackRock BUIDL

The SQL demo in `rwa_tokenization.sql` is modeled directly on the **BlackRock USD Institutional Digital Liquidity Fund (BUIDL)**, the largest tokenized money market fund with over **$2.9B AUM** as of 2024. The sandbox scenario uses a fictional equivalent called the **Blackstone Treasury Token Fund (BTTF)** with $500M AUM.

| Real-World Attribute | BUIDL (BlackRock) | BTTF (This Demo) |
|---|---|---|
| Underlying asset | US Treasury Bills | US Treasury Bills |
| Legal structure | Delaware Trust | Delaware Trust |
| Custodian | BNY Mellon | BNY Mellon Digital Assets |
| Token price | $1.00 (stable NAV) | $1.00 |
| Total supply | 2.9B tokens | 500M tokens |
| Yield mechanism | Daily token rebase | Daily token rebase |
| Investor type | Institutional only | Institutional only |
| KYC platform | Securitize | Securitize (mocked) |
| Blockchain | Ethereum (ERC-1400) | Ethereum (ERC-1400) |
| Transfer restriction | Whitelisted wallets | Whitelisted wallets |

The SQL walkthrough mints tokens to three simulated institutional investors:

| Investor | USD Wired | Tokens Minted |
|---|---|---|
| Citadel Securities | $50,000,000 | 50,000,000 BTTF |
| Fidelity Investments | $25,000,000 | 25,000,000 BTTF |
| Goldman Sachs Asset Mgmt | $10,000,000 | 10,000,000 BTTF |
| **Total** | **$85,000,000** | **85,000,000 BTTF** |

---

## 🧪 Running in a Sandbox Environment

> These instructions are for **local/sandbox use only**. No real assets, KYC data, or investor funds are involved.

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- A Kafka instance (local or Docker)
- Optional: AWS SQS FIFO or a local mock (ElasticMQ) for the signing queue

### 1. Clone the Repository

```bash
git clone https://github.com/pavondunbar/Crypto-Custody-Withdrawal-System-Python.git
cd Crypto-Custody-Withdrawal-System-Python
```

### 2. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate       # macOS/Linux
venv\Scripts\activate          # Windows
```

### 3. Install Dependencies

```bash
pip install asyncpg aiokafka asyncio
```

### 4. Set Up PostgreSQL

Start a local PostgreSQL instance and create a sandbox database:

```bash
psql -U postgres -c "CREATE DATABASE rwa_sandbox;"
psql -U postgres -d rwa_sandbox -f rwa_tokenization.sql
```

The SQL file will:
- Create all tables and indexes
- Register the Blackstone Treasury Token Fund (BTTF)
- Create a Delaware Trust legal wrapper ($1.00/token, 500M total supply)
- Onboard and whitelist three institutional investors (Citadel, Fidelity, Goldman)
- Mint tokens to all three investors
- Run fund-level and investor-level verification queries

### 5. Start a Local Kafka (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest
```

### 6. Run the Outbox Publisher

```python
import asyncio
import asyncpg
from aiokafka import AIOKafkaProducer
from rwa_tokenization import RWAOutboxPublisher

async def main():
    pool = await asyncpg.create_pool("postgresql://postgres@localhost/rwa_sandbox")
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()

    publisher = RWAOutboxPublisher(db=pool, kafka_producer=producer)
    await publisher.run_forever(poll_interval=1)

asyncio.run(main())
```

### 7. Exercise the Core Services with Stubs

```python
from decimal import Decimal
from rwa_tokenization import (
    RWARegistry, LegalWrapperService, KYCComplianceService,
    TokenMintingService, NAVCalculationEngine,
    AssetType, LegalStructure, InvestorTier
)

# Stub implementations for sandbox use
class StubDB:
    # Implement query/execute/transaction backed by local psycopg2
    pass

class StubCustodianRegistry:
    def is_approved(self, custodian): return True

class StubKYCProvider:
    def verify(self, investor_id, tier):
        class Result:
            passed = True
            reference_id = "KYC-SANDBOX-001"
            expiry_date = "2026-01-01"
        return Result()

class StubSanctionsChecker:
    def screen(self, investor_id, jurisdiction):
        class Result:
            is_sanctioned = False
        return Result()

class StubSigningQueue:
    def send(self, **kwargs): print(f"[QUEUE] {kwargs}")

db = StubDB()

# 1. Register a treasury bond asset
registry = RWARegistry(db, StubCustodianRegistry())
asset = registry.register_asset(
    asset_type=AssetType.TREASURY_BOND,
    name="Sandbox T-Bill Fund",
    total_value=Decimal("100000000"),
    jurisdiction="Delaware, United States",
    custodian="BNY Mellon",
    idempotency_key="sandbox-asset-001"
)

# 2. Create a Delaware Trust legal wrapper
legal_svc = LegalWrapperService(db)
wrapper_id = legal_svc.create_legal_wrapper(
    asset_id=asset.id,
    structure_type=LegalStructure.TRUST,
    jurisdiction="Delaware, United States",
    token_supply=Decimal("100000000")   # $1.00/token
)

# 3. Onboard an institutional investor
kyc_svc = KYCComplianceService(db, StubKYCProvider(), StubSanctionsChecker())
compliance_id = kyc_svc.onboard_investor(
    investor_id="investor-uuid-001",
    wallet_address="0xSandboxWallet123",
    jurisdiction="New York, United States",
    investor_tier=InvestorTier.INSTITUTIONAL,
    idempotency_key="kyc-sandbox-001"
)

# 4. Mint tokens on fiat wire receipt
minting_svc = TokenMintingService(db, kyc_svc, StubSigningQueue())
mint = minting_svc.mint_tokens(
    asset_id=asset.id,
    investor_id="investor-uuid-001",
    wallet_address="0xSandboxWallet123",
    token_amount=Decimal("5000000"),
    fiat_received=Decimal("5000000"),
    idempotency_key="mint-sandbox-001"
)
```

### 8. Run the Daily NAV Calculation

```python
from rwa_tokenization import NAVCalculationEngine

nav_engine = NAVCalculationEngine(db, oracle_service=None, signing_queue=StubSigningQueue())
nav_engine.calculate_and_distribute_yield(
    asset_id=asset.id,
    annual_yield_rate=Decimal("0.05"),   # 5% T-bill yield
    idempotency_key=f"nav-2025-01-15"    # one per day
)
```

### 9. Verify the Final State

```sql
-- Fund overview
SELECT a.name, a.total_value, a.custodian,
       lw.structure_type, lw.token_supply, lw.price_per_token
FROM rwa_assets a
JOIN legal_wrappers lw ON lw.asset_id = a.id;

-- Investor positions
SELECT ic.wallet_address, ic.tier, tm.token_amount,
       tm.fiat_received, tm.status, tm.confirmed_at
FROM investor_compliance ic
JOIN token_mints tm ON tm.investor_id = ic.investor_id
ORDER BY tm.token_amount DESC;

-- Fund subscription summary
SELECT a.name, lw.token_supply,
       SUM(tm.token_amount) AS tokens_minted,
       lw.token_supply - SUM(tm.token_amount) AS tokens_remaining,
       ROUND((SUM(tm.token_amount) / lw.token_supply) * 100, 2) AS percent_subscribed
FROM rwa_assets a
JOIN legal_wrappers lw ON lw.asset_id = a.id
JOIN token_mints tm ON tm.asset_id = a.id
WHERE tm.status = 'confirmed'
GROUP BY a.name, lw.token_supply;
```

---

## 📁 Project Structure

```
Crypto-Custody-Withdrawal-System-Python/
│
├── rwa_tokenization.py        # All service classes:
│                              #   RWARegistry
│                              #   LegalWrapperService
│                              #   KYCComplianceService
│                              #   TokenMintingService
│                              #   NAVCalculationEngine
│                              #   TokenRedemptionService
│                              #   RWAReconciliationEngine
│                              #   RWAOutboxPublisher
│
└── rwa_tokenization.sql       # PostgreSQL schema + BTTF end-to-end demo
                               # (BUIDL-inspired: $500M fund, 3 investors)
```

---

## 🚨 Production Warning

**This project is explicitly NOT suitable for production use.** Tokenizing and managing real-world assets is among the most regulated and legally complex activities in financial services. The following critical components are absent or stubbed:

| Missing Component | Risk if Absent |
|---|---|
| Real legal counsel & entity formation | SPV/Trust/Fund has no legal standing |
| Licensed custodian integration | Underlying assets are not actually held or verified |
| Regulated KYC/AML provider (Securitize, etc.) | No actual identity or accreditation verification |
| Real sanctions screening (OFAC API) | Potential sanctions violations and regulatory exposure |
| Smart contract audit (ERC-1400) | Token contract may have exploitable vulnerabilities |
| Securities law compliance | Token may constitute an unregistered securities offering |
| Hardware Security Module (HSM) for signing | Private keys exposed in software |
| Multi-signature approval workflow | Single point of failure for token issuance |
| Custodian API reconciliation | No way to verify on-chain supply matches real-world assets |
| Authentication & authorization | Any caller can register assets, mint, or redeem |
| Dead-letter queue & retry logic | Failed events silently dropped |
| Comprehensive test suite | Untested edge cases in financial state machines |
| Regulatory registration | Operating without required licenses (e.g., SEC, FINRA, MAS) |

> Tokenizing real-world assets requires coordination between securities lawyers, licensed custodians, regulated KYC providers, smart contract auditors, and financial regulators. **Do not use this code to register, tokenize, or manage any real asset, investor funds, or regulated securities.**

---

## 📄 License

This project is provided as-is for educational and reference purposes. Please review the repository's license file before use.

---

*Built with ♥️ by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
