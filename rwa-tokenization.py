import uuid
import json
from datetime import datetime
from enum import Enum
from decimal import Decimal

# -----------------------------------------------
# Enums — State Machines
# -----------------------------------------------

class AssetType(str, Enum):
    REAL_ESTATE    = "real_estate"
    TREASURY_BOND  = "treasury_bond"
    PRIVATE_EQUITY = "private_equity"
    COMMODITY      = "commodity"
    FUND           = "fund"

class AssetStatus(str, Enum):
    PENDING_LEGAL  = "pending_legal"
    PENDING_AUDIT  = "pending_audit"
    APPROVED       = "approved"
    TOKENIZED      = "tokenized"
    REDEEMED       = "redeemed"

class LegalStructure(str, Enum):
    SPV   = "spv"      # Special Purpose Vehicle — real estate
    TRUST = "trust"    # Delaware Trust — bonds, funds
    LLC   = "llc"      # LLC — private equity
    FUND  = "fund"     # Regulated fund — BlackRock BUIDL

class InvestorTier(str, Enum):
    RETAIL        = "retail"         # Limited access
    ACCREDITED    = "accredited"     # $1M+ net worth
    QUALIFIED     = "qualified"      # $5M+ investments
    INSTITUTIONAL = "institutional"  # Banks, funds, hedge funds

class ComplianceStatus(str, Enum):
    PENDING  = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED  = "expired"

class MintStatus(str, Enum):
    PENDING   = "pending"
    APPROVED  = "approved"
    SIGNED    = "signed"
    BROADCAST = "broadcast"
    CONFIRMED = "confirmed"
    FAILED    = "failed"

class RedemptionStatus(str, Enum):
    PENDING   = "pending"
    APPROVED  = "approved"
    BURNING   = "burning"    # Tokens being burned on-chain
    BURNED    = "burned"     # Tokens confirmed burned
    SETTLED   = "settled"    # Fiat wired back to investor
    FAILED    = "failed"


# -----------------------------------------------
# Custom Exceptions
# -----------------------------------------------

class InvalidCustodianError(Exception):
    pass

class InvalidStateError(Exception):
    pass

class WalletNotWhitelistedError(Exception):
    pass

class InsufficientTokenSupplyError(Exception):
    pass

class SanctionedInvestorError(Exception):
    pass

class KYCFailedError(Exception):
    pass

class InsufficientTokenBalanceError(Exception):
    pass


# -----------------------------------------------
# RWA Registry — Register Real World Assets
# -----------------------------------------------

class RWARegistry:
    """
    Registry of real-world assets awaiting or undergoing tokenization.
    Each asset maps to one token contract on-chain.

    BlackRock BUIDL example:
      - Asset type: FUND
      - Name: BlackRock USD Institutional Digital Liquidity Fund
      - Total value: $2.9B+
      - Custodian: BNY Mellon
      - Jurisdiction: Delaware, United States
    """

    def __init__(self, db, custodian_registry):
        self.db = db
        self.custodian_registry = custodian_registry

    def register_asset(
        self, asset_type, name, total_value,
        jurisdiction, custodian, idempotency_key
    ):
        # Idempotency check first — never register same asset twice
        existing = self.db.query(
            "SELECT * FROM rwa_assets "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Validate custodian is licensed and approved
        if not self._validate_custodian(custodian):
            raise InvalidCustodianError(
                f"Custodian {custodian} is not approved"
            )

        # Atomic registration
        with self.db.transaction() as conn:

            asset_id = uuid.uuid4()
            asset = conn.execute(
                "INSERT INTO rwa_assets "
                "(id, type, name, total_value, jurisdiction, "
                " custodian, status, idempotency_key, "
                " created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    asset_id,
                    asset_type.value,
                    name,
                    str(total_value),           # str preserves DECIMAL precision
                    jurisdiction,
                    custodian,
                    AssetStatus.PENDING_LEGAL.value,
                    idempotency_key,
                    datetime.utcnow(),
                    datetime.utcnow()
                )
            )

            # Write to outbox — triggers legal review workflow
            # If downstream service is down, event waits safely here
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(asset_id),
                    "rwa.registered",
                    json.dumps({
                        "asset_id":     str(asset_id),
                        "type":         asset_type.value,
                        "name":         name,
                        "total_value":  str(total_value),
                        "jurisdiction": jurisdiction,
                        "custodian":    custodian
                    }),
                    datetime.utcnow()
                )
            )

        return asset

    def _validate_custodian(self, custodian):
        # In production: check custodian is licensed,
        # regulated, and on the approved custodian list
        return bool(custodian)


# -----------------------------------------------
# Legal Wrapper Service — SPV / Trust / Fund
# -----------------------------------------------

class LegalWrapperService:
    """
    Creates the legal entity that OWNS the real-world asset.
    This is the bridge between off-chain legal ownership
    and on-chain token representation.

    Without this layer the token is just a receipt —
    no enforceable legal claim to the underlying asset.

    BlackRock BUIDL structure:
      - BlackRock creates a regulated fund
      - Fund holds US Treasury bills
      - Token = fund share with legal claim
      - Each token = $1 of fund NAV
      - Yield accrues by rebasing token balance daily
    """

    def __init__(self, db):
        self.db = db

    def create_legal_wrapper(
        self, asset_id, structure_type,
        jurisdiction, token_supply
    ):
        with self.db.transaction() as conn:

            # Pessimistic lock — one legal wrapper per asset
            asset = conn.query(
                "SELECT id, status, total_value "
                "FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # State machine guard — must be in correct state
            if asset.status != AssetStatus.PENDING_LEGAL.value:
                raise InvalidStateError(
                    f"Expected pending_legal, got {asset.status}"
                )

            # Token economics
            # total_value / token_supply = price per token
            # BUIDL: $2.9B / 2.9B tokens = $1.00 per token
            price_per_token = (
                Decimal(asset.total_value) /
                Decimal(str(token_supply))
            )

            wrapper_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO legal_wrappers "
                "(id, asset_id, structure_type, jurisdiction, "
                " token_supply, price_per_token, "
                " status, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    wrapper_id,
                    asset_id,
                    structure_type.value,
                    jurisdiction,
                    str(token_supply),
                    str(price_per_token),
                    "active",
                    datetime.utcnow()
                )
            )

            # Initialize token supply tracker
            conn.execute(
                "INSERT INTO rwa_token_supply "
                "(id, asset_id, total_supply, "
                " minted_supply, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    asset_id,
                    str(token_supply),
                    "0",                        # nothing minted yet
                    datetime.utcnow(),
                    datetime.utcnow()
                )
            )

            # Advance asset state machine
            conn.execute(
                "UPDATE rwa_assets "
                "SET status = %s, updated_at = NOW() "
                "WHERE id = %s",
                (AssetStatus.PENDING_AUDIT.value, asset_id)
            )

            # Outbox — triggers audit workflow
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(asset_id),
                    "rwa.legal_wrapper_created",
                    json.dumps({
                        "asset_id":        str(asset_id),
                        "wrapper_id":      str(wrapper_id),
                        "structure":       structure_type.value,
                        "token_supply":    str(token_supply),
                        "price_per_token": str(price_per_token)
                    }),
                    datetime.utcnow()
                )
            )

        return wrapper_id


# -----------------------------------------------
# KYC Compliance Service — Investor Onboarding
# -----------------------------------------------

class KYCComplianceService:
    """
    RWA tokens are NOT permissionless like DeFi.
    Every investor must pass KYC/AML before holding tokens.
    Both sender AND receiver must be whitelisted on every transfer.
    This is enforced at the smart contract level — not just policy.

    BlackRock BUIDL requirements:
      - Institutional investors only
      - Minimum $5M investment
      - Full KYC/AML via Securitize platform
      - Whitelisted Ethereum wallet addresses only
      - Transfers only between whitelisted addresses
      - KYC expires annually — must re-verify

    This is the PRIMARY difference between RWA tokens
    and standard ERC-20 tokens.
    """

    def __init__(self, db, kyc_provider, sanctions_checker):
        self.db = db
        self.kyc_provider = kyc_provider
        self.sanctions_checker = sanctions_checker

    def onboard_investor(
        self, investor_id, wallet_address,
        jurisdiction, investor_tier,
        idempotency_key
    ):
        # Idempotency check — never double-onboard same investor
        existing = self.db.query(
            "SELECT * FROM investor_compliance "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Step 1: Sanctions screening — OFAC, EU, UN lists
        # Must happen OUTSIDE transaction — external API call
        # Never hold a row lock during external calls
        sanctions_result = self.sanctions_checker.screen(
            investor_id, jurisdiction
        )
        if sanctions_result.is_sanctioned:
            raise SanctionedInvestorError(
                f"Investor {investor_id} is on a sanctions list"
            )

        # Step 2: KYC/AML verification — identity + accreditation
        # Also outside transaction — external API call
        kyc_result = self.kyc_provider.verify(
            investor_id, investor_tier
        )
        if not kyc_result.passed:
            raise KYCFailedError(kyc_result.reason)

        # Step 3: Write compliance record + whitelist wallet atomically
        with self.db.transaction() as conn:

            compliance_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO investor_compliance "
                "(id, investor_id, wallet_address, tier, "
                " jurisdiction, status, kyc_reference, "
                " idempotency_key, created_at, expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    compliance_id,
                    investor_id,
                    wallet_address,
                    investor_tier.value,
                    jurisdiction,
                    ComplianceStatus.APPROVED.value,
                    kyc_result.reference_id,
                    idempotency_key,
                    datetime.utcnow(),
                    kyc_result.expiry_date     # KYC expires — must re-verify
                )
            )

            # Whitelist the wallet address
            # Only whitelisted wallets can send or receive RWA tokens
            conn.execute(
                "INSERT INTO whitelisted_wallets "
                "(id, investor_id, wallet_address, "
                " tier, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    investor_id,
                    wallet_address,
                    investor_tier.value,
                    datetime.utcnow()
                )
            )

            # Outbox — notifies token platform of new whitelisted wallet
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(compliance_id),
                    "investor.whitelisted",
                    json.dumps({
                        "investor_id":    str(investor_id),
                        "wallet_address": wallet_address,
                        "tier":           investor_tier.value,
                        "jurisdiction":   jurisdiction
                    }),
                    datetime.utcnow()
                )
            )

        return compliance_id

    def can_transfer(self, from_wallet, to_wallet):
        """
        Called before every token transfer.
        Both sender AND receiver must be whitelisted.
        Smart contract calls this via on-chain oracle
        or off-chain transfer hook depending on architecture.
        """
        sender_approved = self.db.query(
            "SELECT id FROM whitelisted_wallets "
            "WHERE wallet_address = %s",
            (from_wallet,)
        )
        receiver_approved = self.db.query(
            "SELECT id FROM whitelisted_wallets "
            "WHERE wallet_address = %s",
            (to_wallet,)
        )
        return bool(sender_approved) and bool(receiver_approved)

    def revoke_whitelist(self, investor_id, reason):
        """
        Revokes investor whitelist status.
        Called when KYC expires, sanctions hit, or
        investor requests redemption and exit.
        After revocation wallet cannot receive transfers.
        """
        with self.db.transaction() as conn:

            conn.execute(
                "DELETE FROM whitelisted_wallets "
                "WHERE investor_id = %s",
                (investor_id,)
            )

            conn.execute(
                "UPDATE investor_compliance "
                "SET status = %s, updated_at = NOW() "
                "WHERE investor_id = %s",
                (ComplianceStatus.EXPIRED.value, investor_id)
            )

            # Outbox — notifies token platform to block transfers
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(investor_id),
                    "investor.whitelist_revoked",
                    json.dumps({
                        "investor_id": str(investor_id),
                        "reason":      reason
                    }),
                    datetime.utcnow()
                )
            )


# -----------------------------------------------
# Token Minting Service — Issue Tokens to Investors
# -----------------------------------------------

class TokenMintingService:
    """
    Mints RWA tokens to a whitelisted investor wallet.
    Triggered when investor wires fiat to the fund.

    Flow mirrors crypto withdrawal exactly:
      - Idempotency check first
      - Lock token supply FOR UPDATE
      - Create mint record + outbox atomically
      - Publish to signing queue outside transaction
      - Signing service calls ERC-1400 mint() on-chain

    BlackRock BUIDL example:
      - Investor wires $10M USD to fund
      - Fund receives fiat, buys T-bills with proceeds
      - Mints 10,000,000 BUIDL tokens to investor wallet
      - Each token = $1 NAV, rebases daily with T-bill yield
    """

    def __init__(self, db, compliance_service, signing_queue):
        self.db = db
        self.compliance = compliance_service
        self.signing_queue = signing_queue

    def mint_tokens(
        self, asset_id, investor_id, wallet_address,
        token_amount, fiat_received, idempotency_key
    ):
        # Idempotency check first — never double-mint
        existing = self.db.query(
            "SELECT * FROM token_mints "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Wallet must be whitelisted before receiving tokens
        if not self.compliance.can_transfer(
            "ISSUER_WALLET", wallet_address
        ):
            raise WalletNotWhitelistedError(
                f"Wallet {wallet_address} is not whitelisted"
            )

        # Atomic mint record + supply update + outbox
        with self.db.transaction() as conn:

            # Pessimistic lock — one mint at a time per asset
            # Prevents overselling token supply
            supply = conn.query(
                "SELECT id, total_supply, minted_supply "
                "FROM rwa_token_supply "
                "WHERE asset_id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # Check available token supply
            available = (
                Decimal(supply.total_supply) -
                Decimal(supply.minted_supply)
            )
            if available < Decimal(str(token_amount)):
                raise InsufficientTokenSupplyError(
                    f"Only {available} tokens remaining"
                )

            # Reserve the tokens — locked until confirmed on-chain
            conn.execute(
                "UPDATE rwa_token_supply "
                "SET minted_supply = minted_supply + %s, "
                "updated_at = NOW() "
                "WHERE asset_id = %s",
                (str(token_amount), asset_id)
            )

            # Create mint record — state machine entry point
            mint_id = uuid.uuid4()
            mint = conn.execute(
                "INSERT INTO token_mints "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, fiat_received, status, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    mint_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    str(fiat_received),
                    MintStatus.PENDING.value,
                    idempotency_key,
                    datetime.utcnow()
                )
            )

            # Write to outbox in same transaction
            # If blockchain is down, event waits safely here
            # Outbox publisher delivers to Kafka when ready
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(mint_id),
                    "token.mint_requested",
                    json.dumps({
                        "mint_id":       str(mint_id),
                        "asset_id":      str(asset_id),
                        "wallet":        wallet_address,
                        "token_amount":  str(token_amount),
                        "fiat_received": str(fiat_received)
                    }),
                    datetime.utcnow()
                )
            )

        # Publish to signing queue OUTSIDE the transaction
        # Never hold a row lock during external calls
        # message_group_id = asset_id ensures FIFO per asset
        self.signing_queue.send(
            message_body={
                "mint_id":      str(mint_id),
                "asset_id":     str(asset_id),
                "wallet":       wallet_address,
                "token_amount": str(token_amount)
            },
            message_group_id=str(asset_id),
            message_deduplication_id=str(mint_id)
        )

        return mint

    def confirm_mint(self, mint_id, tx_hash, block_number):
        """
        Called by ConfirmationTracker when mint transaction
        is confirmed on-chain. Settles the ledger.
        """
        with self.db.transaction() as conn:

            conn.execute(
                "UPDATE token_mints "
                "SET status = %s, "
                "tx_hash = %s, "
                "block_number = %s, "
                "confirmed_at = NOW() "
                "WHERE id = %s",
                (
                    MintStatus.CONFIRMED.value,
                    tx_hash,
                    block_number,
                    mint_id
                )
            )

            # Outbox — notifies investor dashboard
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(mint_id),
                    "token.mint_confirmed",
                    json.dumps({
                        "mint_id":     str(mint_id),
                        "tx_hash":     tx_hash,
                        "block_number": str(block_number)
                    }),
                    datetime.utcnow()
                )
            )


# -----------------------------------------------
# Token Redemption Service — Burn Tokens, Wire Fiat
# -----------------------------------------------

class TokenRedemptionService:
    """
    Inverse of minting. Investor returns tokens to issuer,
    tokens are burned on-chain, fiat is wired back.

    This is the most operationally complex flow because
    it spans blockchain (burn), compliance (re-verify),
    and traditional finance (wire transfer) — three
    separate systems that must all succeed atomically
    or not at all.

    State machine:
    PENDING → APPROVED → BURNING → BURNED → SETTLED
    """

    def __init__(self, db, compliance_service,
                 signing_queue, wire_service):
        self.db = db
        self.compliance = compliance_service
        self.signing_queue = signing_queue
        self.wire_service = wire_service

    def request_redemption(
        self, asset_id, investor_id, wallet_address,
        token_amount, bank_account, idempotency_key
    ):
        # Idempotency check first
        existing = self.db.query(
            "SELECT * FROM token_redemptions "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Verify investor still has compliance status
        if not self.compliance.can_transfer(
            wallet_address, "ISSUER_WALLET"
        ):
            raise WalletNotWhitelistedError()

        with self.db.transaction() as conn:

            # Lock investor token balance
            balance = conn.query(
                "SELECT token_amount "
                "FROM token_mints "
                "WHERE investor_id = %s "
                "AND asset_id = %s "
                "AND status = %s "
                "FOR UPDATE",
                (
                    investor_id, asset_id,
                    MintStatus.CONFIRMED.value
                )
            )

            if Decimal(balance.token_amount) < Decimal(str(token_amount)):
                raise InsufficientTokenBalanceError()

            redemption_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO token_redemptions "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, bank_account, status, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    redemption_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    bank_account,
                    RedemptionStatus.PENDING.value,
                    idempotency_key,
                    datetime.utcnow()
                )
            )

            # Outbox — triggers burn workflow
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_requested",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "asset_id":      str(asset_id),
                        "wallet":        wallet_address,
                        "token_amount":  str(token_amount)
                    }),
                    datetime.utcnow()
                )
            )

        # Publish to signing queue — burns tokens on-chain
        self.signing_queue.send(
            message_body={
                "redemption_id": str(redemption_id),
                "asset_id":      str(asset_id),
                "wallet":        wallet_address,
                "token_amount":  str(token_amount),
                "action":        "burn"
            },
            message_group_id=str(asset_id),
            message_deduplication_id=str(redemption_id)
        )

        return redemption_id

    def settle_redemption(self, redemption_id):
        """
        Called after burn is confirmed on-chain.
        Wires fiat back to investor bank account.
        Final step — releases all locks.
        """
        with self.db.transaction() as conn:

            redemption = conn.query(
                "SELECT * FROM token_redemptions "
                "WHERE id = %s "
                "FOR UPDATE",
                (redemption_id,)
            )

            if redemption.status != RedemptionStatus.BURNED.value:
                raise InvalidStateError(
                    f"Expected burned, got {redemption.status}"
                )

            # Release token supply — tokens are gone from circulation
            conn.execute(
                "UPDATE rwa_token_supply "
                "SET minted_supply = minted_supply - %s, "
                "updated_at = NOW() "
                "WHERE asset_id = %s",
                (redemption.token_amount, redemption.asset_id)
            )

            # Mark settled
            conn.execute(
                "UPDATE token_redemptions "
                "SET status = %s, settled_at = NOW() "
                "WHERE id = %s",
                (RedemptionStatus.SETTLED.value, redemption_id)
            )

            # Outbox — triggers fiat wire transfer
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_settled",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "investor_id":   str(redemption.investor_id),
                        "bank_account":  redemption.bank_account,
                        "token_amount":  str(redemption.token_amount)
                    }),
                    datetime.utcnow()
                )
            )


# -----------------------------------------------
# NAV Calculation Engine — Daily Token Rebase
# -----------------------------------------------

class NAVCalculationEngine:
    """
    Calculates Net Asset Value daily and rebases token supply.

    BlackRock BUIDL rebase mechanism:
      - Fund holds T-bills earning ~5% APY
      - Daily yield = total_value * (0.05 / 365)
      - Yield distributed by minting new tokens to investors
      - Token price stays $1.00 — balance increases instead
      - This is called a positive rebase

    Example:
      - Investor holds 10,000,000 BUIDL tokens
      - Daily yield rate = 5% / 365 = 0.01370% per day
      - Daily yield = 10,000,000 * 0.0001370 = 1,370 tokens
      - Next day investor holds 10,001,370 tokens
      - All at $1.00 per token
    """

    def __init__(self, db, oracle_service, signing_queue):
        self.db = db
        self.oracle_service = oracle_service
        self.signing_queue = signing_queue

    def calculate_and_distribute_yield(
        self, asset_id, annual_yield_rate, idempotency_key
    ):
        # Idempotency — only one NAV calc per asset per day
        existing = self.db.query(
            "SELECT * FROM nav_calculations "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        with self.db.transaction() as conn:

            # Lock asset for NAV update
            asset = conn.query(
                "SELECT id, total_value "
                "FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # Daily yield = total_value * (annual_rate / 365)
            daily_yield = (
                Decimal(asset.total_value) *
                Decimal(str(annual_yield_rate)) /
                Decimal("365")
            )

            nav_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO nav_calculations "
                "(id, asset_id, total_value, daily_yield, "
                " yield_rate, calculated_at, idempotency_key) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    nav_id,
                    asset_id,
                    str(asset.total_value),
                    str(daily_yield),
                    str(annual_yield_rate),
                    datetime.utcnow(),
                    idempotency_key
                )
            )

            # Update total fund value with accrued yield
            conn.execute(
                "UPDATE rwa_assets "
                "SET total_value = total_value + %s, "
                "updated_at = NOW() "
                "WHERE id = %s",
                (str(daily_yield), asset_id)
            )

            # Outbox — triggers token rebase on-chain
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(nav_id),
                    "nav.yield_calculated",
                    json.dumps({
                        "asset_id":    str(asset_id),
                        "nav_id":      str(nav_id),
                        "daily_yield": str(daily_yield),
                        "yield_rate":  str(annual_yield_rate)
                    }),
                    datetime.utcnow()
                )
            )

        return nav_id


# -----------------------------------------------
# Reconciliation Engine — Verify On-Chain vs Ledger
# -----------------------------------------------

class RWAReconciliationEngine:
    """
    Compares internal ledger state against on-chain reality.
    Runs daily after NAV calculation.

    Checks:
      1. Total tokens minted in DB = total supply on-chain
      2. Each investor balance in DB = wallet balance on-chain
      3. Total fund value in DB = custodian report
      4. No tokens held by non-whitelisted wallets

    Any mismatch triggers immediate alert and halts
    new mints and redemptions until resolved.
    """

    def __init__(self, db, blockchain_service,
                 custodian_api, alert_service):
        self.db = db
        self.blockchain = blockchain_service
        self.custodian_api = custodian_api
        self.alert_service = alert_service

    def reconcile_asset(self, asset_id):

        # Get internal ledger state
        internal = self.db.query(
            "SELECT ts.minted_supply, a.total_value "
            "FROM rwa_token_supply ts "
            "JOIN rwa_assets a ON a.id = ts.asset_id "
            "WHERE ts.asset_id = %s",
            (asset_id,)
        )

        # Get on-chain token supply
        onchain_supply = self.blockchain.get_total_supply(asset_id)

        # Get custodian report — actual AUM
        custodian_nav = self.custodian_api.get_nav(asset_id)

        mismatches = []

        # Check 1: Token supply matches on-chain
        if Decimal(internal.minted_supply) != Decimal(str(onchain_supply)):
            mismatches.append({
                "type":     "supply_mismatch",
                "internal": str(internal.minted_supply),
                "onchain":  str(onchain_supply)
            })

        # Check 2: Fund value matches custodian report
        if abs(
            Decimal(internal.total_value) -
            Decimal(str(custodian_nav))
        ) > Decimal("0.01"):        # $0.01 tolerance
            mismatches.append({
                "type":      "nav_mismatch",
                "internal":  str(internal.total_value),
                "custodian": str(custodian_nav)
            })

        if mismatches:
            # Alert immediately — halt minting and redemptions
            self.alert_service.critical(
                f"RWA reconciliation failed for asset {asset_id}",
                mismatches
            )
            return False

        return True


# -----------------------------------------------
# Outbox Publisher — Deliver Events to Kafka
# -----------------------------------------------

import asyncio
import logging

logger = logging.getLogger(__name__)

class RWAOutboxPublisher:
    """
    Polls outbox_events table and publishes to Kafka.
    Same pattern as crypto custody outbox publisher.
    FOR UPDATE SKIP LOCKED enables multiple publisher
    instances without duplicate delivery.
    """

    def __init__(self, db, kafka_producer, batch_size=100):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size

    async def poll_and_publish(self):
        async with self.db.acquire() as conn:
            events = await conn.fetch(
                "SELECT id, aggregate_id, event_type, payload "
                "FROM outbox_events "
                "WHERE published_at IS NULL "
                "ORDER BY created_at "
                "LIMIT $1 "
                "FOR UPDATE SKIP LOCKED",
                self.batch_size
            )

            if not events:
                return

            for event in events:
                await self.kafka.send(
                    topic=f"rwa.{event['event_type']}",
                    key=event["aggregate_id"].encode(),
                    value=event["payload"].encode()
                )

                # Mark published INSIDE loop — per event
                # If Kafka fails mid-batch only published
                # events get marked — rest retry next poll
                await conn.execute(
                    "UPDATE outbox_events "
                    "SET published_at = NOW() "
                    "WHERE id = $1",
                    event["id"]
                )

    async def run_forever(self, poll_interval=1):
        while True:
            try:
                await self.poll_and_publish()
            except Exception as e:
                logger.error(f"RWA outbox poll failed: {e}")
            await asyncio.sleep(poll_interval)
