from datetime import date
from sqlalchemy import text

from scripts.pricing_yahoo import get_close_price

def update_market_price(engine) -> int:
    """
    Update market_price for ALL STOCKS in Portfolio
    """
    updated = 0

    with engine.begin() as conn:
        # 1️⃣ LẤY DANH SÁCH TICKER CỔ PHIẾU
        tickers = conn.execute(
            text("""
                SELECT DISTINCT ticker
                FROM Portfolio
                WHERE asset_type = 'Stock'
                  AND ticker IS NOT NULL
            """)
        ).scalars().all()

        # 2️⃣ UPDATE GIÁ
        for ticker in tickers:
            try:
                price = get_close_price(ticker)

                conn.execute(
                    text("""
                        UPDATE Portfolio
                        SET market_price = :price,
                            snapshot_date = :d
                        WHERE ticker = :ticker
                    """),
                    {
                        "price": price,
                        "d": date.today(),
                        "ticker": ticker
                    }
                )

                updated += 1

            except Exception as e:
                print(f"[WARN] {ticker}: {e}")

    return updated

def update_portfolio_after_trade(engine, ticker, side, quantity, price, trade_date):
    side = side.capitalize()  # Buy / Sell

    with engine.begin() as conn:

        # --- LẤY TRẠNG THÁI HIỆN TẠI ---
        r = conn.execute(
            text("""
                SELECT quantity, buy_price, market_price
                FROM portfolio
                WHERE ticker = :ticker
                FOR UPDATE
            """),
            {"ticker": ticker}
        ).mappings().first()

        if r is None:
            raise ValueError(f"Ticker {ticker} chưa tồn tại trong portfolio")

        current_qty = r["quantity"]
        current_buy = r["buy_price"]
        market_price = r["market_price"]

        # --- UPDATE QTY & BUY PRICE ---
        if side == "Buy":
            new_qty = current_qty + quantity
            new_buy_price = (
                (current_qty * current_buy + quantity * price) / new_qty
                if new_qty > 0 else current_buy
            )

        elif side == "Sell":
            new_qty = current_qty - quantity
            if new_qty < 0:
                raise ValueError(f"Sell vượt position: {ticker}")
            new_buy_price = current_buy  # ❗ không đổi

        else:
            raise ValueError("Side must be Buy or Sell")

        # --- INTEREST (UNREALIZED RETURN %) ---
        if new_qty > 0 and new_buy_price > 0:
            interest = (market_price - new_buy_price) / new_buy_price
        else:
            interest = 0

        # --- TOTAL PORTFOLIO VALUE (READ ONLY) ---
        total_value = conn.execute(
            text("""
                SELECT COALESCE(SUM(quantity * market_price), 0)
                FROM portfolio
            """)
        ).scalar()

        position_value = new_qty * market_price
        current_weight = position_value / total_value if total_value > 0 else 0

        # --- UPDATE DUY NHẤT ---
        conn.execute(
            text("""
                UPDATE portfolio
                SET
                    price_date     = :d,
                    quantity       = :q,
                    buy_price      = :bp,
                    interest       = :i,
                    current_weight = :w
                WHERE ticker = :ticker
            """),
            {
                "ticker": ticker,
                "d": trade_date,
                "q": new_qty,
                "bp": new_buy_price,
                "i": interest,
                "w": current_weight
            }
        )

    return {
        "ticker": ticker,
        "quantity": new_qty,
        "buy_price": new_buy_price,
        "interest": interest,
        "current_weight": current_weight
    }