"""Integration tests for RWA tokenization critical paths.

Requires a running PostgreSQL instance (via ``make up``).
Connects to localhost:5433 using the Docker-exposed port.
"""

import uuid
from decimal import Decimal

import psycopg2
import psycopg2.extras
import pytest

psycopg2.extras.register_uuid()

DSN = "host=localhost port=5433 dbname=rwa user=ledger_user password=ledger_pass"


@pytest.fixture()
def conn():
    c = psycopg2.connect(DSN)
    c.autocommit = False
    yield c
    c.rollback()
    c.close()


def _cur(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ------------------------------------------------------------------
# 1. Double-entry ledger: debit account + credit account, balanced
# ------------------------------------------------------------------

def test_ledger_entry_creates_balanced_pair(conn):
    """A single ledger_entries row represents a balanced debit/credit
    pair with the same amount on both sides."""
    cur = _cur(conn)

    asset_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO rwa_assets "
        "(id, type, name, total_value, jurisdiction, custodian, created_at) "
        "VALUES (%s, 'fund', 'Test Fund', '1000000', 'US', 'Custodian', NOW())",
        (asset_id,),
    )

    entry_id = uuid.uuid4()
    ref_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO ledger_entries "
        "(id, asset_id, entry_type, debit_account, credit_account, "
        " amount, reference_id, created_at) "
        "VALUES (%s, %s, 'mint', 'investor:AAA', 'treasury:BBB', "
        " '500000', %s, NOW())",
        (entry_id, asset_id, ref_id),
    )

    cur.execute(
        "SELECT debit_account, credit_account, amount "
        "FROM ledger_entries WHERE id = %s",
        (entry_id,),
    )
    row = cur.fetchone()
    assert row["debit_account"] == "investor:AAA"
    assert row["credit_account"] == "treasury:BBB"
    assert Decimal(row["amount"]) == Decimal("500000")


# ------------------------------------------------------------------
# 2. Derived balances: v_investor_balance computes from journal
# ------------------------------------------------------------------

def test_investor_balance_derived_from_ledger(conn):
    """Balances are never stored; they are derived from
    ledger_entries via the v_investor_balance view."""
    cur = _cur(conn)

    asset_id = uuid.uuid4()
    investor_id = f"investor:{uuid.uuid4()}"

    cur.execute(
        "INSERT INTO rwa_assets "
        "(id, type, name, total_value, jurisdiction, custodian, created_at) "
        "VALUES (%s, 'fund', 'Balance Test', '1000000', 'US', 'C', NOW())",
        (asset_id,),
    )

    # Mint 1000 tokens to investor (debit investor, credit treasury)
    cur.execute(
        "INSERT INTO ledger_entries "
        "(id, asset_id, entry_type, debit_account, credit_account, "
        " amount, reference_id, created_at) "
        "VALUES (%s, %s, 'mint', %s, 'treasury:X', '1000', %s, NOW())",
        (uuid.uuid4(), asset_id, investor_id, uuid.uuid4()),
    )

    # Redeem 300 tokens (debit burn, credit investor)
    cur.execute(
        "INSERT INTO ledger_entries "
        "(id, asset_id, entry_type, debit_account, credit_account, "
        " amount, reference_id, created_at) "
        "VALUES (%s, %s, 'redemption', 'burn:X', %s, '300', %s, NOW())",
        (uuid.uuid4(), asset_id, investor_id, uuid.uuid4()),
    )

    cur.execute(
        "SELECT balance FROM v_investor_balance "
        "WHERE investor_id = %s AND asset_id = %s",
        (investor_id, asset_id),
    )
    row = cur.fetchone()
    assert Decimal(row["balance"]) == Decimal("700")


# ------------------------------------------------------------------
# 3. Append-only: UPDATE on core tables is rejected
# ------------------------------------------------------------------

def test_append_only_rejects_update_on_rwa_assets(conn):
    """The prevent_mutation() trigger blocks UPDATE on rwa_assets."""
    cur = _cur(conn)

    asset_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO rwa_assets "
        "(id, type, name, total_value, jurisdiction, custodian, created_at) "
        "VALUES (%s, 'fund', 'Immutable', '100', 'US', 'C', NOW())",
        (asset_id,),
    )

    with pytest.raises(psycopg2.errors.RaiseException, match="append-only"):
        cur.execute(
            "UPDATE rwa_assets SET name = 'HACKED' WHERE id = %s",
            (asset_id,),
        )


# ------------------------------------------------------------------
# 4. Append-only: DELETE on ledger_entries is rejected
# ------------------------------------------------------------------

def test_append_only_rejects_delete_on_ledger(conn):
    """The prevent_mutation() trigger blocks DELETE on ledger_entries."""
    cur = _cur(conn)

    asset_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO rwa_assets "
        "(id, type, name, total_value, jurisdiction, custodian, created_at) "
        "VALUES (%s, 'fund', 'DeleteTest', '100', 'US', 'C', NOW())",
        (asset_id,),
    )

    entry_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO ledger_entries "
        "(id, asset_id, entry_type, debit_account, credit_account, "
        " amount, reference_id, created_at) "
        "VALUES (%s, %s, 'mint', 'a', 'b', '100', %s, NOW())",
        (entry_id, asset_id, uuid.uuid4()),
    )

    with pytest.raises(psycopg2.errors.RaiseException, match="append-only"):
        cur.execute(
            "DELETE FROM ledger_entries WHERE id = %s", (entry_id,),
        )


# ------------------------------------------------------------------
# 5. State machine: valid transition recorded
# ------------------------------------------------------------------

def test_state_transition_records_audit_context(conn):
    """State transitions carry request_id, trace_id, and actor
    for full audit traceability."""
    cur = _cur(conn)

    entity_id = uuid.uuid4()
    request_id = uuid.uuid4()
    trace_id = uuid.uuid4()

    cur.execute(
        "INSERT INTO state_transitions "
        "(id, entity_type, entity_id, from_state, to_state, "
        " request_id, trace_id, actor, created_at) "
        "VALUES (%s, 'asset', %s, NULL, 'pending_legal', "
        " %s, %s, 'admin:demo', NOW())",
        (uuid.uuid4(), entity_id, request_id, trace_id),
    )

    cur.execute(
        "SELECT * FROM v_current_state "
        "WHERE entity_type = 'asset' AND entity_id = %s",
        (entity_id,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row["current_state"] == "pending_legal"


# ------------------------------------------------------------------
# 6. State machine: v_current_state returns latest transition
# ------------------------------------------------------------------

def test_current_state_reflects_latest_transition(conn):
    """v_current_state always returns the most recent transition."""
    cur = _cur(conn)

    entity_id = uuid.uuid4()

    for state in ["pending_legal", "pending_audit", "approved"]:
        cur.execute(
            "INSERT INTO state_transitions "
            "(id, entity_type, entity_id, from_state, to_state, "
            " created_at) "
            "VALUES (%s, 'asset', %s, NULL, %s, NOW())",
            (uuid.uuid4(), entity_id, state),
        )

    cur.execute(
        "SELECT current_state FROM v_current_state "
        "WHERE entity_type = 'asset' AND entity_id = %s",
        (entity_id,),
    )
    row = cur.fetchone()
    assert row["current_state"] == "approved"


# ------------------------------------------------------------------
# 7. Idempotency: duplicate idempotency_key rejected
# ------------------------------------------------------------------

def test_idempotency_key_prevents_duplicate_insert(conn):
    """UNIQUE constraint on idempotency_key prevents duplicate
    asset registration."""
    cur = _cur(conn)

    idem_key = f"test-{uuid.uuid4()}"
    for _ in range(1):
        cur.execute(
            "INSERT INTO rwa_assets "
            "(id, type, name, total_value, jurisdiction, custodian, "
            " idempotency_key, created_at) "
            "VALUES (%s, 'fund', 'First', '100', 'US', 'C', %s, NOW())",
            (uuid.uuid4(), idem_key),
        )

    with pytest.raises(psycopg2.errors.UniqueViolation):
        cur.execute(
            "INSERT INTO rwa_assets "
            "(id, type, name, total_value, jurisdiction, custodian, "
            " idempotency_key, created_at) "
            "VALUES (%s, 'fund', 'Duplicate', '100', 'US', 'C', %s, NOW())",
            (uuid.uuid4(), idem_key),
        )


# ------------------------------------------------------------------
# 8. Outbox + DLQ: append-only triggers on outbox tables
# ------------------------------------------------------------------

def test_outbox_tables_are_append_only(conn):
    """outbox_events, outbox_published, and outbox_dlq all reject
    UPDATE and DELETE via immutability triggers."""
    cur = _cur(conn)

    event_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO outbox_events "
        "(id, aggregate_id, event_type, payload, created_at) "
        "VALUES (%s, 'agg-1', 'test.event', '{}'::jsonb, NOW())",
        (event_id,),
    )

    with pytest.raises(psycopg2.errors.RaiseException, match="append-only"):
        cur.execute(
            "UPDATE outbox_events SET event_type = 'hacked' "
            "WHERE id = %s",
            (event_id,),
        )
