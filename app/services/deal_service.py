"""
Deal service: vault-level lease deals, collateral locks, yield distribution.
All mutation delegates to PostgreSQL stored procedures.
"""

from decimal import Decimal
from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)


def _ensure_user_counterparty(cur, username: str) -> int:
    cur.execute(
        """
        INSERT INTO counterparties (name, entity_type, credit_rating, max_exposure_grams, is_active)
        VALUES (%s, 'TRADING_DESK', 'USER', 1000000000, TRUE)
        ON CONFLICT (name) DO UPDATE SET is_active = TRUE
        RETURNING id
        """,
        [username],
    )
    return cur.fetchone()["id"]


def list_counterparties(active_only: bool = True) -> list[dict]:
    query = """
        SELECT c.id, c.name, c.entity_type, c.credit_rating,
               c.max_exposure_grams, c.is_active, c.created_at,
               COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0) AS current_exposure_grams
        FROM counterparties c
        LEFT JOIN allocation_deals d ON d.counterparty_id = c.id
    """
    if active_only:
        query += " WHERE c.is_active = TRUE"
    query += " GROUP BY c.id ORDER BY c.name"
    with get_cursor() as (conn, cur):
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def create_counterparty(name: str, entity_type: str, credit_rating: str = None,
                        max_exposure_grams: float = 0) -> dict:
    valid = {"JEWELLER", "REFINER", "TRADING_DESK"}
    if entity_type.upper() not in valid:
        raise ValueError(f"entity_type must be one of {valid}")
    if max_exposure_grams < 0:
        raise ValueError("max_exposure_grams cannot be negative")
    with get_cursor() as (conn, cur):
        cur.execute(
            "INSERT INTO counterparties (name, entity_type, credit_rating, max_exposure_grams) "
            "VALUES (%s, %s::counterparty_entity_type, %s, %s) "
            "RETURNING id, name, entity_type, credit_rating, max_exposure_grams, created_at",
            [name, entity_type.upper(), credit_rating, Decimal(str(max_exposure_grams))],
        )
        return dict(cur.fetchone())


def transfer_gold_to_user(from_account_id: int, from_username: str, target_username: str,
                          amount_grams: float, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Amount must be positive")
    amount = Decimal(str(amount_grams))

    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [from_account_id])
        from_account = cur.fetchone()
        if not from_account:
            raise ValueError(f"Account not found: id={from_account_id}")

        cur.execute(
            """
            SELECT a.id AS account_id, u.username
            FROM users u
            JOIN accounts a ON a.user_id = u.id
            WHERE u.username = %s AND u.is_active = TRUE
            FOR UPDATE OF a
            """,
            [target_username],
        )
        target = cur.fetchone()
        if not target:
            raise ValueError(f"Target user not found or inactive: username={target_username}")
        if target["account_id"] == from_account_id:
            raise ValueError("Target user must be different from source user")
        if from_account["balance_grams"] < amount:
            raise ValueError(
                f"Insufficient available balance for transfer: requested={amount} available={from_account['balance_grams']}"
            )

        from_new_balance = from_account["balance_grams"] - amount
        cur.execute(
            "UPDATE accounts SET balance_grams = %s, total_withdrawn_grams = total_withdrawn_grams + %s WHERE id = %s",
            [from_new_balance, amount, from_account_id],
        )
        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
            VALUES (%s, 'WITHDRAW', %s, %s, %s)
            RETURNING id
            """,
            [from_account_id, amount, from_new_balance,
             notes or f"Gold transfer to @{target_username} from @{from_username}"],
        )
        from_ledger_id = cur.fetchone()["id"]

        cur.execute("SELECT balance_grams FROM accounts WHERE id = %s", [target["account_id"]])
        target_account = cur.fetchone()
        target_new_balance = target_account["balance_grams"] + amount
        cur.execute(
            "UPDATE accounts SET balance_grams = %s, total_deposited_grams = total_deposited_grams + %s WHERE id = %s",
            [target_new_balance, amount, target["account_id"]],
        )
        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, notes)
            VALUES (%s, 'DEPOSIT', %s, %s, %s)
            RETURNING id
            """,
            [target["account_id"], amount, target_new_balance,
             notes or f"Gold transfer from @{from_username} to @{target_username}"],
        )
        to_ledger_id = cur.fetchone()["id"]

    return {
        "from_account_id": from_account_id,
        "to_account_id": target["account_id"],
        "amount_grams": float(amount),
        "from_ledger_event_id": from_ledger_id,
        "to_ledger_event_id": to_ledger_id,
    }


def open_lease_deal(account_id: int, amount_grams: float, yield_rate_bps: int,
                    maturity_date: str = None, deal_reference: str = None, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Amount must be positive")
    if not 0 <= yield_rate_bps <= 10000:
        raise ValueError("yield_rate_bps must be 0–10000")
    amount = Decimal(str(amount_grams))

    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT value::NUMERIC AS min_deal_amount_grams FROM system_config WHERE key = 'min_deal_amount_grams'"
        )
        min_amount_row = cur.fetchone()
        min_amount = min_amount_row["min_deal_amount_grams"] if min_amount_row and min_amount_row["min_deal_amount_grams"] is not None else Decimal("1.0")
        if amount < min_amount:
            raise ValueError(f"Amount {amount} is below minimum deal size ({min_amount})")

        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [account_id])
        borrower_account = cur.fetchone()
        if not borrower_account:
            raise ValueError(f"Account not found: id={account_id}")

        cur.execute("SELECT username FROM users WHERE id = %s", [borrower_account["user_id"]])
        borrower_user = cur.fetchone()
        borrower_username = borrower_user["username"]

        cur.execute("SELECT a.id AS id FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username = '__system__' FOR UPDATE")
        sys_row = cur.fetchone()
        if not sys_row:
            raise ValueError("System account not found")
        sys_account_id = sys_row["id"]
        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [sys_account_id])
        system_account = cur.fetchone()
        if system_account["balance_grams"] < amount:
            raise ValueError(
                f"Insufficient bank balance for lease: requested={amount} available={system_account['balance_grams']}"
            )

        counterparty_id = _ensure_user_counterparty(cur, borrower_username)

        cur.execute(
            """
            INSERT INTO allocations (account_id, allocation_type, status, amount_grams, yield_rate_bps, is_pooled, maturity_date, notes)
            VALUES (%s, 'LEASING', 'ACTIVE', %s, %s, FALSE, %s::DATE, %s)
            RETURNING id
            """,
            [account_id, amount, yield_rate_bps, maturity_date, notes],
        )
        allocation_id = cur.fetchone()["id"]

        cur.execute(
            """
            INSERT INTO allocation_deals (allocation_id, counterparty_id, amount_grams, yield_rate_bps, start_date, maturity_date, deal_reference, notes)
            VALUES (%s, %s, %s, %s, CURRENT_DATE, %s::DATE, %s, %s)
            RETURNING id
            """,
            [allocation_id, counterparty_id, amount, yield_rate_bps, maturity_date, deal_reference, notes],
        )
        deal_id = cur.fetchone()["id"]

        bank_new_balance = system_account["balance_grams"] - amount
        cur.execute(
            "UPDATE accounts SET balance_grams = %s, allocated_grams = allocated_grams + %s WHERE id = %s",
            [bank_new_balance, amount, sys_account_id],
        )

        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
            VALUES (%s, 'ALLOCATE', %s, %s, %s, %s, %s)
            RETURNING id
            """,
            [
                sys_account_id,
                amount,
                bank_new_balance,
                allocation_id,
                deal_id,
                notes or f"Bank lease opened to @{borrower_username}: deal_id={deal_id}",
            ],
        )
        ledger_event_id = cur.fetchone()["id"]

        cur.execute(
            "UPDATE accounts SET allocated_grams = allocated_grams + %s WHERE id = %s",
            [amount, account_id],
        )

        cur.execute(
            "SELECT d.*, c.name AS counterparty_name FROM allocation_deals d "
            "JOIN counterparties c ON c.id = d.counterparty_id WHERE d.id = %s",
            [deal_id],
        )
        deal = cur.fetchone()

    logger.info("LEASE OPENED: deal_id=%s borrower=%s amount=%s", deal_id, borrower_username, amount_grams)
    return {"allocation_id": allocation_id, "deal_id": deal_id,
            "ledger_event_id": ledger_event_id, "deal": dict(deal)}


def open_collateral_lock(account_id: int, amount_grams: float, yield_rate_bps: int,
                         maturity_date: str = None, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Amount must be positive")
    if yield_rate_bps < 0:
        raise ValueError("yield_rate_bps cannot be negative")
    with get_cursor() as (conn, cur):
        amount = Decimal(str(amount_grams))
        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [account_id])
        account = cur.fetchone()
        if not account:
            raise ValueError(f"Account not found: id={account_id}")
        if account["balance_grams"] < amount:
            raise ValueError(f"Insufficient available balance for collateral lock: requested={amount} available={account['balance_grams']}")

        cur.execute(
            """
            INSERT INTO allocations (account_id, allocation_type, status, amount_grams, yield_rate_bps, is_pooled, maturity_date, notes)
            VALUES (%s, 'COLLATERAL_LOCK', 'ACTIVE', %s, %s, FALSE, %s::DATE, %s)
            RETURNING id
            """,
            [account_id, amount, yield_rate_bps, maturity_date, notes],
        )
        allocation_id = cur.fetchone()["id"]

        new_balance = account["balance_grams"] - amount
        cur.execute("UPDATE accounts SET balance_grams = %s WHERE id = %s", [new_balance, account_id])
        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
            VALUES (%s, 'WITHDRAW', %s, %s, %s, %s)
            RETURNING id
            """,
            [account_id, amount, new_balance, allocation_id,
             notes or f"Collateral lock opened: allocation_id={allocation_id}"],
        )
        ledger_event_id = cur.fetchone()["id"]

    return {"allocation_id": allocation_id, "ledger_event_id": ledger_event_id}


def close_deal(account_id: int, deal_id: int, is_default: bool = False, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM allocation_deals WHERE id = %s FOR UPDATE", [deal_id])
        deal = cur.fetchone()
        if not deal:
            raise ValueError(f"Deal not found: id={deal_id}")
        if deal["status"] != "ACTIVE":
            raise ValueError(f"Deal id={deal_id} is not ACTIVE (status={deal['status']}). Cannot close.")

        cur.execute("SELECT * FROM allocations WHERE id = %s FOR UPDATE", [deal["allocation_id"]])
        allocation = cur.fetchone()
        if not allocation:
            raise ValueError(f"Allocation not found for deal id={deal_id}")
        if allocation["account_id"] != account_id:
            raise ValueError(f"Deal id={deal_id} does not belong to account_id={account_id}")

        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [account_id])
        source_account = cur.fetchone()

        cur.execute("SELECT name FROM counterparties WHERE id = %s", [deal["counterparty_id"]])
        target_counterparty = cur.fetchone()
        target_username = target_counterparty["name"] if target_counterparty else None
        if not target_username:
            raise ValueError(f"Target user mapping missing for deal id={deal_id}")

        cur.execute(
            """
            SELECT a.id AS account_id, a.balance_grams
            FROM users u JOIN accounts a ON a.user_id = u.id
            WHERE u.username = %s AND u.is_active = TRUE
            FOR UPDATE OF a
            """,
            [target_username],
        )
        target_account = cur.fetchone()
        if not target_account:
            raise ValueError(f"Target user for deal not found or inactive: username={target_username}")

        from datetime import date
        days_held = max((date.today() - deal["start_date"]).days, 1)
        amount = Decimal(str(deal["amount_grams"]))
        if is_default:
            gross_yield = Decimal("0")
            close_status = "DEFAULTED"
        else:
            gross_yield = (amount * Decimal(deal["yield_rate_bps"]) / Decimal("10000") * Decimal(days_held) / Decimal("365")).quantize(Decimal("0.000001"))
            close_status = "MATURED"

        cur.execute("SELECT value::INTEGER AS fee_bps FROM system_config WHERE key = 'system_fee_bps'")
        fee_row = cur.fetchone()
        fee_bps = fee_row["fee_bps"] if fee_row and fee_row["fee_bps"] is not None else 2500
        fee_grams = (gross_yield * Decimal(fee_bps) / Decimal("10000")).quantize(Decimal("0.000001"))
        distributable = gross_yield - fee_grams

        required_target_balance = amount + (gross_yield if gross_yield > 0 else Decimal("0"))
        if target_account["balance_grams"] < required_target_balance:
            raise ValueError(
                f"Target user has insufficient balance to settle deal: required={required_target_balance} available={target_account['balance_grams']}"
            )

        cur.execute("SELECT a.id AS id FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username = '__system__' FOR UPDATE")
        sys_row = cur.fetchone()
        if not sys_row:
            raise ValueError("System account not found")
        sys_account_id = sys_row["id"]
        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [sys_account_id])
        bank_account = cur.fetchone()

        principal_balance = bank_account["balance_grams"] + amount
        cur.execute(
            "UPDATE accounts SET balance_grams = %s, allocated_grams = allocated_grams - %s WHERE id = %s",
            [principal_balance, amount, sys_account_id],
        )
        target_after_principal = target_account["balance_grams"] - amount
        cur.execute(
            "UPDATE accounts SET balance_grams = %s, allocated_grams = allocated_grams - %s, total_withdrawn_grams = total_withdrawn_grams + %s WHERE id = %s",
            [target_after_principal, amount, amount, target_account["account_id"]],
        )
        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
            VALUES (%s, 'DEAL_CLOSE', %s, %s, %s, %s, %s)
            RETURNING id
            """,
            [
                sys_account_id,
                amount,
                principal_balance,
                allocation["id"],
                deal_id,
                notes or f"Deal {deal_id} closed: bank principal received from @{target_username}, days_held={days_held}",
            ],
        )
        ledger_close_id = cur.fetchone()["id"]

        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
            VALUES (%s, 'WITHDRAW', %s, %s, %s, %s, %s)
            """,
            [
                target_account["account_id"],
                amount,
                target_after_principal,
                allocation["id"],
                deal_id,
                f"Deal {deal_id} principal settled to account_id={account_id}",
            ],
        )

        ledger_yield_id = None
        if gross_yield > 0:
            target_after_yield = target_after_principal - gross_yield
            cur.execute(
                "UPDATE accounts SET balance_grams = %s, total_withdrawn_grams = total_withdrawn_grams + %s WHERE id = %s",
                [target_after_yield, gross_yield, target_account["account_id"]],
            )
            cur.execute(
                """
                INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
                VALUES (%s, 'WITHDRAW', %s, %s, %s, %s, %s)
                RETURNING id
                """,
                [
                    target_account["account_id"],
                    gross_yield,
                    target_after_yield,
                    allocation["id"],
                    deal_id,
                    f"Deal {deal_id} yield paid: gross={gross_yield} fee_bps={fee_bps}",
                ],
            )
            ledger_yield_id = cur.fetchone()["id"]

            cur.execute("SELECT a.id AS id FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username = '__system__' FOR UPDATE")
            sys_row = cur.fetchone()
            if not sys_row:
                raise ValueError("System account not found")
            sys_account_id = sys_row["id"]
            cur.execute("SELECT balance_grams FROM accounts WHERE id = %s FOR UPDATE", [sys_account_id])
            sys_account = cur.fetchone()
            system_new_balance = sys_account["balance_grams"] + gross_yield
            cur.execute(
                "UPDATE accounts SET balance_grams = %s, total_deposited_grams = total_deposited_grams + %s WHERE id = %s",
                [system_new_balance, gross_yield, sys_account_id],
            )
            cur.execute(
                """
                INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, deal_id, notes)
                VALUES (%s, 'YIELD_EARNED', %s, %s, %s, %s, %s)
                RETURNING id
                """,
                [
                    sys_account_id,
                    gross_yield,
                    system_new_balance,
                    allocation["id"],
                    deal_id,
                    f"Yield earned from {target_username}: {gross_yield}g gross, {fee_bps} bps fee, {distributable}g distributable",
                ],
            )
            ledger_yield_id = cur.fetchone()["id"]
            cur.execute(
                "UPDATE system_state SET system_profit_grams = system_profit_grams + %s, last_updated_at = NOW() WHERE id = 1",
                [fee_grams],
            )

        cur.execute("UPDATE allocations SET status = 'DEALLOCATED', deallocated_at = NOW(), closed_at = NOW() WHERE id = %s", [allocation["id"]])
        cur.execute("UPDATE allocation_deals SET status = %s::deal_status, closed_at = NOW() WHERE id = %s", [close_status, deal_id])

        cur.execute(
            "SELECT COALESCE(SUM(a.balance_grams), 0) AS total_user_bal "
            "FROM accounts a JOIN users u ON u.id = a.user_id "
            "WHERE u.username <> '__system__' AND a.balance_grams > 0"
        )
        total_user_bal = cur.fetchone()["total_user_bal"]

        cur.execute(
            """
            INSERT INTO yield_events (deal_id, allocation_id, gross_yield_grams, system_fee_grams, distributable_yield_grams, system_fee_bps, total_user_balance_snapshot, distribution_completed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE)
            RETURNING id
            """,
            [deal_id, allocation["id"], max(gross_yield, Decimal("0")), fee_grams, max(distributable, Decimal("0")), fee_bps, total_user_bal],
        )
        yield_event_id = cur.fetchone()["id"]

        cur.execute(
            "SELECT * FROM yield_events WHERE id = %s",
            [yield_event_id],
        )
        ye = cur.fetchone()

    logger.info("DEAL CLOSED: deal_id=%s gross_yield=%s", deal_id, gross_yield)
    return {"yield_event_id": yield_event_id, "gross_yield_grams": float(gross_yield),
            "ledger_close_id": ledger_close_id, "ledger_yield_id": ledger_yield_id,
            "yield_event": dict(ye)}


def close_collateral_lock(account_id: int, allocation_id: int, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM allocations WHERE id = %s FOR UPDATE", [allocation_id])
        allocation = cur.fetchone()
        if not allocation:
            raise ValueError(f"Allocation not found: id={allocation_id}")
        if allocation["allocation_type"] != "COLLATERAL_LOCK":
            raise ValueError(f"Allocation id={allocation_id} is not COLLATERAL_LOCK")
        if allocation["status"] != "ACTIVE":
            raise ValueError(f"Allocation id={allocation_id} is not ACTIVE")
        if allocation["account_id"] != account_id:
            raise ValueError(f"Allocation id={allocation_id} does not belong to account_id={account_id}")

        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [account_id])
        account = cur.fetchone()
        if not account:
            raise ValueError(f"Account not found: id={account_id}")

        cur.execute("SELECT a.id AS id FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username = '__system__' FOR UPDATE")
        system_row = cur.fetchone()
        if not system_row:
            raise ValueError("System account not found")
        system_account_id = system_row["id"]

        cur.execute("SELECT * FROM accounts WHERE id = %s FOR UPDATE", [system_account_id])
        system_account = cur.fetchone()

        from datetime import date
        days_held = max((date.today() - allocation["allocated_at"].date()).days, 1)
        gross_yield = round(float(allocation["amount_grams"]) * int(allocation["yield_rate_bps"]) / 10000.0 * days_held / 365.0, 6)

        cur.execute("SELECT value::INTEGER AS system_fee_bps FROM system_config WHERE key = 'system_fee_bps'")
        fee_row = cur.fetchone()
        fee_bps = fee_row["system_fee_bps"] if fee_row and fee_row["system_fee_bps"] is not None else 2500
        fee_grams = round(gross_yield * fee_bps / 10000.0, 6)
        distributable = gross_yield - fee_grams

        principal_balance = account["balance_grams"] + allocation["amount_grams"]
        cur.execute("UPDATE accounts SET balance_grams = %s WHERE id = %s", [principal_balance, account_id])
        cur.execute(
            """
            INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
            VALUES (%s, 'DEPOSIT', %s, %s, %s, %s)
            RETURNING id
            """,
            [account_id, allocation["amount_grams"], principal_balance, allocation_id,
             notes or f"Collateral lock closed: allocation_id={allocation_id}"],
        )
        ledger_dealloc_id = cur.fetchone()["id"]

        ledger_yield_id = None
        if gross_yield > 0:
            system_balance = system_account["balance_grams"] + gross_yield
            cur.execute(
                "UPDATE accounts SET balance_grams = %s, total_deposited_grams = total_deposited_grams + %s WHERE id = %s",
                [system_balance, gross_yield, system_account_id],
            )
            cur.execute(
                """
                INSERT INTO ledger_events (account_id, event_type, amount_grams, balance_after, reference_id, notes)
                VALUES (%s, 'YIELD_EARNED', %s, %s, %s, %s)
                RETURNING id
                """,
                [system_account_id, gross_yield, system_balance, allocation_id,
                 f"Collateral yield: {gross_yield} gross over {days_held} days"],
            )
            ledger_yield_id = cur.fetchone()["id"]
            cur.execute("UPDATE system_state SET system_profit_grams = system_profit_grams + %s, last_updated_at = NOW() WHERE id = 1", [fee_grams])

        cur.execute("UPDATE allocations SET status = 'DEALLOCATED', deallocated_at = NOW(), closed_at = NOW() WHERE id = %s", [allocation_id])
        cur.execute(
            "SELECT COALESCE(SUM(a.balance_grams), 0) AS total_user_balance FROM accounts a JOIN users u ON u.id = a.user_id WHERE u.username <> '__system__' AND a.balance_grams > 0"
        )
        total_user_balance = cur.fetchone()["total_user_balance"]
        cur.execute(
            """
            INSERT INTO yield_events (
                deal_id, allocation_id, gross_yield_grams, system_fee_grams,
                distributable_yield_grams, system_fee_bps, total_user_balance_snapshot, distribution_completed
            )
            VALUES (NULL, %s, %s, %s, %s, %s, %s, FALSE)
            RETURNING id
            """,
            [allocation_id, gross_yield, fee_grams, distributable, fee_bps, total_user_balance],
        )
        yield_event_id = cur.fetchone()["id"]

    return {
        "yield_event_id": yield_event_id,
        "gross_yield_grams": gross_yield,
        "ledger_dealloc_id": ledger_dealloc_id,
        "ledger_yield_id": ledger_yield_id,
    }


def distribute_yield(yield_event_id: int) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM yield_events WHERE id = %s FOR UPDATE", [yield_event_id])
        yield_event = cur.fetchone()
        if not yield_event:
            raise ValueError(f"yield_event not found: id={yield_event_id}")
        if yield_event["distribution_completed"]:
            raise ValueError(f"yield_event id={yield_event_id} has already been distributed.")
        if yield_event["distributable_yield_grams"] <= 0:
            cur.execute(
                "UPDATE yield_events SET distribution_completed = TRUE, distributed_at = NOW() WHERE id = %s",
                [yield_event_id],
            )
            return {"yield_event_id": yield_event_id, "users_credited": 0}

        cur.execute(
            """
            SELECT COALESCE(SUM(a.balance_grams), 0) AS total_balance
            FROM accounts a JOIN users u ON u.id = a.user_id
            WHERE u.username <> '__system__' AND a.balance_grams > 0
            """
        )
        total_balance = cur.fetchone()["total_balance"]
        if total_balance <= 0:
            raise ValueError("No user balances found to distribute yield to.")

        cur.execute(
            """
            SELECT a.id AS account_id, a.balance_grams, u.username
            FROM accounts a JOIN users u ON u.id = a.user_id
            WHERE u.username <> '__system__' AND a.balance_grams > 0
            ORDER BY a.id
            FOR UPDATE OF a
            """
        )
        user_accounts = cur.fetchall()

        cur.execute(
            "SELECT a.id AS account_id, a.balance_grams FROM accounts a JOIN users u ON u.id = a.user_id "
            "WHERE u.username = '__system__' FOR UPDATE"
        )
        system_account = cur.fetchone()
        if not system_account:
            raise ValueError("System account not found")

        users_credited = 0
        sum_distributed = Decimal("0")
        for user_account in user_accounts:
            share = Decimal(str(user_account["balance_grams"])) / Decimal(str(total_balance))
            user_yield = Decimal(str(round(float(yield_event["distributable_yield_grams"]) * float(share), 6)))
            if user_yield <= 0:
                continue
            new_balance = Decimal(str(user_account["balance_grams"])) + user_yield
            cur.execute(
                "UPDATE accounts SET balance_grams = %s, total_deposited_grams = total_deposited_grams + %s WHERE id = %s",
                [new_balance, user_yield, user_account["account_id"]],
            )
            cur.execute(
                """
                INSERT INTO yield_distributions (yield_event_id, account_id, user_balance_snapshot, share_fraction, yield_grams, ledger_event_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    yield_event_id,
                    user_account["account_id"],
                    user_account["balance_grams"],
                    share,
                    user_yield,
                    None,
                ],
            )
            sum_distributed += user_yield
            users_credited += 1

        new_system_balance = Decimal(str(system_account["balance_grams"])) - sum_distributed
        if new_system_balance < 0:
            raise ValueError(
                f"System account has insufficient balance to distribute yield event {yield_event_id}: "
                f"required={sum_distributed} available={system_account['balance_grams']}"
            )
        cur.execute(
            "UPDATE accounts SET balance_grams = %s WHERE id = %s",
            [new_system_balance, system_account["account_id"]],
        )

        if abs(sum_distributed - Decimal(str(yield_event["distributable_yield_grams"]))) > Decimal("0.01"):
            raise ValueError(
                f"Yield distribution rounding slack too large: expected={yield_event['distributable_yield_grams']} distributed={sum_distributed} slack={Decimal(str(yield_event['distributable_yield_grams'])) - sum_distributed}"
            )

        cur.execute(
            "UPDATE system_state SET total_yield_distributed_grams = total_yield_distributed_grams + %s, last_updated_at = NOW() WHERE id = 1",
            [sum_distributed],
        )

        cur.execute(
            "UPDATE yield_events SET distribution_completed = TRUE, distributed_at = NOW() WHERE id = %s",
            [yield_event_id],
        )
    logger.info("YIELD DISTRIBUTED: yield_event_id=%s users_credited=%s", yield_event_id, users_credited)
    return {"yield_event_id": yield_event_id, "users_credited": users_credited}


def get_active_deals() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT d.id AS deal_id, d.deal_reference, d.status,
                   u.username AS counterparty, c.entity_type, c.credit_rating,
                   a.allocation_type, d.amount_grams, d.yield_rate_bps,
                   d.start_date, d.maturity_date,
                   CURRENT_DATE - d.start_date AS days_active,
                   ROUND(d.amount_grams * d.yield_rate_bps / 10000.0 * (CURRENT_DATE - d.start_date) / 365.0, 6) AS accrued_yield_grams,
                   ROUND(d.amount_grams + (d.amount_grams * d.yield_rate_bps / 10000.0 * (CURRENT_DATE - d.start_date) / 365.0), 6) AS current_payoff_grams,
                   d.created_at
            FROM allocation_deals d
            JOIN counterparties c ON c.id = d.counterparty_id
            JOIN users u          ON u.username = c.name AND u.is_active = TRUE
            JOIN allocations a    ON a.id = d.allocation_id
            WHERE d.status = 'ACTIVE'
            ORDER BY d.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_all_deals() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT d.id AS deal_id, d.deal_reference, d.status,
                   u.username AS counterparty, c.entity_type,
                   d.amount_grams, d.yield_rate_bps,
                   d.start_date, d.maturity_date, d.closed_at, d.created_at
            FROM allocation_deals d
            JOIN counterparties c ON c.id = d.counterparty_id
            JOIN users u          ON u.username = c.name AND u.is_active = TRUE
            ORDER BY d.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_active_collateral_locks(account_id: int) -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT a.id AS allocation_id, a.amount_grams, a.yield_rate_bps,
                   a.allocated_at, a.maturity_date,
                   CURRENT_DATE - a.allocated_at::DATE AS days_held,
                   ROUND(a.amount_grams * a.yield_rate_bps / 10000.0 * (CURRENT_DATE - a.allocated_at::DATE) / 365.0, 6) AS accrued_yield_grams,
                   a.notes
            FROM allocations a
            WHERE a.account_id = %s AND a.allocation_type = 'COLLATERAL_LOCK' AND a.status = 'ACTIVE' AND a.is_pooled = FALSE
            ORDER BY a.allocated_at
        """, [account_id])
        return [dict(r) for r in cur.fetchall()]


def get_counterparty_exposure() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT c.id, c.name, c.entity_type, c.credit_rating,
                   COUNT(d.id) FILTER (WHERE d.status='ACTIVE') AS active_deals,
                   COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0) AS gold_held_grams,
                   c.max_exposure_grams,
                   ROUND(COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0)
                         / NULLIF(c.max_exposure_grams, 0) * 100, 2) AS pct_of_limit
            FROM counterparties c
            JOIN users u ON u.username = c.name AND u.is_active = TRUE
            LEFT JOIN allocation_deals d ON d.counterparty_id = c.id
            WHERE c.is_active = TRUE
            GROUP BY c.id, c.name, c.entity_type, c.credit_rating, c.max_exposure_grams
            ORDER BY gold_held_grams DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_pending_yield_events() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT ye.id AS yield_event_id, d.id AS deal_id, d.deal_reference,
                   c.name AS counterparty,
                   ye.gross_yield_grams, ye.system_fee_grams,
                   ye.distributable_yield_grams, ye.created_at AS earned_at
            FROM yield_events ye
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            LEFT JOIN counterparties c   ON c.id = d.counterparty_id
            WHERE ye.distribution_completed = FALSE AND ye.distributable_yield_grams > 0
            ORDER BY ye.created_at ASC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_yield_history() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT ye.id, ye.gross_yield_grams, ye.system_fee_grams,
                   ye.distributable_yield_grams, ye.system_fee_bps,
                   ye.distribution_completed, ye.created_at, ye.distributed_at,
                   d.deal_reference, c.name AS counterparty
            FROM yield_events ye
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            LEFT JOIN counterparties c   ON c.id = d.counterparty_id
            ORDER BY ye.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_user_yield_history(account_id: int) -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT yd.id, yd.yield_event_id, ye.gross_yield_grams AS event_gross_yield,
                   yd.user_balance_snapshot, yd.share_fraction, yd.yield_grams,
                   yd.created_at, d.deal_reference
            FROM yield_distributions yd
            JOIN yield_events ye ON ye.id = yd.yield_event_id
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            WHERE yd.account_id = %s
            ORDER BY yd.created_at DESC
        """, [account_id])
        return [dict(r) for r in cur.fetchall()]


def get_vault_balance() -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM system_state WHERE id = 1")
        return dict(cur.fetchone())
