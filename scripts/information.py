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
# ======================
def load_investor_portfolio(customer_id: str):
    engine = get_engine()


    with engine.connect() as conn:
        # 1️⃣ Thông tin tổng hợp
        investor = conn.execute(
            text("""
                SELECT
                    customer_name,
                    nos,
                    capital
                FROM investors
                WHERE customer_id = :cid
            """),
            {"cid": customer_id}
        ).mappings().fetchone()


        if investor is None:
            return None


        # 2️⃣ Giá CCQ hiện tại (nav_per_unit)
        nav_per_unit = conn.execute(
            text("""
                SELECT nav_per_unit
                FROM nav
                ORDER BY nav_date DESC
                LIMIT 1
            """)
        ).scalar()

        if nav_per_unit is None:
            nav_per_unit = 0


        market_value = investor["nos"] * nav_per_unit
        total_assets = market_value
        pnl = market_value - investor["capital"]
        roi = (
            pnl / investor["capital"] * 100
            if investor["capital"] and investor["capital"] > 0
            else 0
        )


        # 3️⃣ Lịch sử giao dịch CCQ
        trades = pd.read_sql(
            """
            SELECT
                trade_date,
                side,
                quantity,
                price,
                cost,
                cash_flow,
                current_fs
            FROM fundshare_trades
            WHERE customer_id = %(cid)s
            ORDER BY trade_date DESC
            """,
            conn,
            params={"cid": customer_id}
        )


    return {
        "customer_name": investor["customer_name"],
        "nos": investor["nos"],
        "capital": investor["capital"],
        "nav_per_unit": nav_per_unit,
        "market_value": market_value,
        "total_assets": total_assets,
        "pnl": pnl,
        "roi": roi,
        "trades": trades,
    }




