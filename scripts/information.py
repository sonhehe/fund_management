# scripts/information.py
import pandas as pd
from sqlalchemy import text
from scripts.db_engine import get_engine








# ==============
# OVERALL INFO
# ==============
        # ======
        # ADMIN
        # ======
def load_admin_information():
    engine = get_engine()




    with engine.connect() as conn:
        # 1️⃣ Cash của quỹ
        cash = conn.execute(
            text("""
                SELECT COALESCE(SUM(net_value), 0)
                FROM portfolio
                WHERE asset_type = 'Cash'
            """)
        ).scalar()




        # 2️⃣ Fund share tổng
        fund = pd.read_sql(
            """
            SELECT
                SUM(quantity) AS total_ccq,
                SUM(quantity * market_price) AS market_value,
                SUM(quantity * buy_price) AS invested_value
            FROM portfolio
            WHERE asset_type = 'Fund share'
            """,
            conn
        )




        # 3️⃣ Interest toàn quỹ
        interest = conn.execute(
            text("""
                SELECT interest
                FROM overall_snapshot
                WHERE attribute = 'Fund share'
                ORDER BY snapshot_time DESC
                LIMIT 1
            """)
        ).scalar()




        # 4️⃣ Danh sách nhà đầu tư
        investors = pd.read_sql(
            """
            SELECT
                customer_id,
                customer_name,
                nos,
                capital,
                current_cash,
                status
            FROM investors
            ORDER BY customer_id
            """,
            conn
        )




    return {
        "cash": cash,
        "total_ccq": fund["total_ccq"].iloc[0] or 0,
        "market_value": fund["market_value"].iloc[0] or 0,
        "invested_value": fund["invested_value"].iloc[0] or 0,
        "interest": interest or 0,
        "investors": investors,
    }




        # ========
        # INVESTOR
        # ========




def load_investor_information(customer_id: str):
    engine = get_engine()




    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    customer_id,
                    customer_name,
                    email,
                    phone,
                    address,
                    bank_account,
                    open_account_date,
                    status
                FROM investors
                WHERE customer_id = :cid
            """),
            {"cid": customer_id}
        ).mappings().fetchone()




    if row is None:
        return None




    return dict(row)
# ======================
# INVESTOR – PORTFOLIO
from decimal import Decimal

def load_investor_portfolio(customer_id: str):
    engine = get_engine()

    with engine.connect() as conn:

        investor = conn.execute(
            text("""
                SELECT customer_name, current_cash
                FROM investors
                WHERE customer_id = :cid
            """),
            {"cid": customer_id}
        ).mappings().fetchone()

        if investor is None:
            return None

        nav_per_unit = conn.execute(
            text("""
                SELECT nav_per_unit
                FROM nav
                ORDER BY nav_date DESC
                LIMIT 1
            """)
        ).scalar()

        trades = pd.read_sql(
            """
            SELECT trade_date, side, quantity, price
            FROM fundshare_trades
            WHERE customer_id = %(cid)s
            ORDER BY trade_date
            """,
            conn,
            params={"cid": customer_id}
        )
        cash_requests = pd.read_sql(
            """
            SELECT
                created_at,
                type,
                amount,
                status
            FROM cash_requests
            WHERE customer_id = %(cid)s
            AND status = 'SUCCESS'
            ORDER BY created_at DESC
            """,
            conn,
            params={"cid": customer_id}
    )
    # =============================
    # SAFE TYPE CONVERSION
    # =============================

    nav_per_unit = float(nav_per_unit or 0)
    current_cash = float(investor["current_cash"] or 0)

    # =============================
    # FIFO ACCOUNTING
    # =============================

    inventory = []
    realized_pnl = 0.0
    total_buy_cash = 0.0

    for _, row in trades.iterrows():

        qty = float(row["quantity"] or 0)
        price = float(row["price"] or 0)

        if row["side"] == "BUY":
            inventory.append({"qty": qty, "price": price})
            total_buy_cash += qty * price

        elif row["side"] == "SELL":

            sell_qty = qty

            while sell_qty > 0 and inventory:
                lot = inventory[0]

                take = min(sell_qty, lot["qty"])

                realized_pnl += take * (price - lot["price"])

                lot["qty"] -= take
                sell_qty -= take

                if lot["qty"] <= 1e-9:
                    inventory.pop(0)

    # =============================
    # POSITION CALCULATION
    # =============================

    total_units = float(sum(lot["qty"] for lot in inventory))
    cost_remaining = float(sum(lot["qty"] * lot["price"] for lot in inventory))

    market_value = float(total_units * nav_per_unit)
    unrealized_pnl = float(market_value - cost_remaining)
    total_pnl = float(realized_pnl + unrealized_pnl)

    total_assets = float(market_value + current_cash)

    # =============================
    # ROI (giữ nguyên logic bạn đang dùng)
    # =============================

    net_invested = float(total_buy_cash)

    roi = (total_pnl / net_invested * 100) if net_invested > 0 else 0.0

    return {
        "customer_name": investor["customer_name"],
        "nos": total_units,
        "nav_per_unit": nav_per_unit,
        "market_value": market_value,
        "cost_basis_remaining": cost_remaining,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "total_pnl": total_pnl,
        "roi": roi,
        "current_cash": current_cash,
        "total_assets": total_assets,
        "trades": trades,
        "cash_requests": cash_requests,
    }
