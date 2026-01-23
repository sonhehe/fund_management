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
