from datetime import datetime
from sqlalchemy import text
from scripts.db_engine import get_engine




# ======================
# LẤY NAV / CCQ
# ======================
def get_latest_nav_per_unit() -> float:
    engine = get_engine()
    with engine.connect() as conn:
        nav = conn.execute(
            text("""
                SELECT nav_per_unit
                FROM nav
                ORDER BY nav_date DESC
                LIMIT 1
            """)
        ).scalar()


    if nav is None:
        raise ValueError("NAV chưa được tính")


    return float(nav)




# ======================
# LẤY PHÍ QUẢN LÝ (CHUNG BUY / SELL)
# ======================
def get_fundshare_fee_rate(side=None) -> float:
    if side is None:
        side = "BUY"
    engine = get_engine()


    # tạm thời dùng chung 1 loại phí
    cost_type = "transaction_fee"


    with engine.connect() as conn:
        rate = conn.execute(
            text("""
                SELECT rate
                FROM costs
                WHERE cost_type = :ctype
                ORDER BY cost_date DESC
                LIMIT 1
            """),
            {"ctype": cost_type}
        ).scalar()


    if rate is None:
        raise ValueError("Fee rate not found")


    return float(rate)




# ======================
# TÍNH PHÍ
# ======================
def calculate_fundshare_fee(side: str, amount: float) -> float:
    rate = get_fundshare_fee_rate(side)
    return amount * rate




# ======================
# MUA / BÁN CCQ (CORE LOGIC)
# ======================
def execute_fundshare_trade(
    customer_id: str,
    side: str,
    amount: float | None = None,
    quantity: float | None = None
):
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")


    engine = get_engine()
    nav = get_latest_nav_per_unit()
    fee_rate = get_fundshare_fee_rate(side)


    with engine.begin() as conn:


        # ======================
        # 1️⃣ LẤY INVESTOR (LOCK)
        # ======================
        investor = conn.execute(
            text("""
                SELECT nos, capital
                FROM investors
                WHERE customer_id = :cid
                FOR UPDATE
            """),
            {"cid": customer_id}
        ).mappings().fetchone()


        if investor is None:
            raise ValueError("Investor not found")


        current_nos = float(investor["nos"])
        current_capital = float(investor["capital"])


        # ======================
        # 2️⃣ LẤY CASH QUỸ (YTM)
        # ======================
        cash_row = conn.execute(
            text("""
                SELECT net_value
                FROM portfolio
                WHERE ticker = 'YTM'
                FOR UPDATE
            """)
        ).fetchone()


        if cash_row is None:
            raise ValueError("Cash (YTM) not found in portfolio")


        fund_cash = float(cash_row[0])


        # ======================
        # 3️⃣ BUY = GÓP VỐN
        # ======================
        if side == "BUY":
            if amount is None or amount <= 0:
                raise ValueError("Amount must be provided for BUY")


            fee = amount * fee_rate
            net_amount = amount - fee
            units = net_amount / nav


            new_nos = current_nos + units
            new_capital = current_capital + net_amount


            # quỹ nhận tiền
            cash_change = net_amount


        # ======================
        # 4️⃣ SELL = BÁN CCQ
        # ======================
        else:
            if quantity is None or quantity <= 0:
                raise ValueError("Quantity must be provided for SELL")


            if quantity > current_nos:
                raise ValueError("Not enough fund shares to sell")


            gross_amount = quantity * nav
            fee = gross_amount * fee_rate
            net_amount = gross_amount - fee


            if fund_cash < net_amount:
                raise ValueError("Fund does not have enough cash")


            new_nos = current_nos - quantity
            new_capital = current_capital - gross_amount


            # quỹ trả tiền
            cash_change = -net_amount
            units = quantity


        # ======================
        # 5️⃣ GHI TRADE
        # ======================
        conn.execute(
            text("""
                INSERT INTO fundshare_trades (
                    trade_date,
                    customer_id,
                    side,
                    quantity,
                    price,
                    cost,
                    cash_flow,
                    current_fs
                )
                VALUES (
                    :d, :cid, :side,
                    :qty, :price, :fee,
                    :cf, :fs
                )
            """),
            {
                "d": datetime.now(),
                "cid": customer_id,
                "side": side,
                "qty": units,
                "price": nav,
                "fee": fee,
                "cf": cash_change,
                "fs": new_nos
            }
        )


        # ======================
        # 6️⃣ UPDATE INVESTOR
        # ======================
        conn.execute(
            text("""
                UPDATE investors
                SET nos = :nos,
                    capital = :cap
                WHERE customer_id = :cid
            """),
            {
                "nos": new_nos,
                "cap": new_capital,
                "cid": customer_id
            }
        )


        # ======================
        # 7️⃣ UPDATE CASH (YTM)
        # ======================
        conn.execute(
            text("""
                UPDATE portfolio
                SET net_value = net_value + :delta
                WHERE ticker = 'YTM'
            """),
            {"delta": cash_change}
        )



