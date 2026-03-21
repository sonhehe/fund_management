import yfinance as yf
from sqlalchemy import text
import datetime

# ==============================
# 1️⃣ LẤY GIÁ
# ==============================
def get_close_price(ticker: str) -> dict:

    ticker_clean = ticker.upper().replace(".VN", "")
    yf_ticker = ticker_clean + ".VN"

    data = yf.Ticker(yf_ticker).history(period="7d")

    if data.empty:
        raise ValueError(f"No data for {ticker}")

    last = data.iloc[-1]

    market_date = last.name.date()
    today = datetime.date.today()

    return {
        "ticker": ticker_clean,
        "close_price": float(last["Close"]),
        "price_date": market_date,     # ngày thị trường
        "updated_at": today           # ngày hệ thống update
    }

# ==============================
# 2️⃣ LẤY DANH SÁCH STOCK
# ==============================
def get_stock_tickers(engine):

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT DISTINCT ticker
                FROM portfolio
                WHERE asset_type = 'Stock'
                AND ticker IS NOT NULL
            """)
        )

        tickers = [row[0] for row in result]

    return tickers


# ==============================
# 3️⃣ UPDATE ALL
# ==============================
def update_all_prices(engine):

    tickers = get_stock_tickers(engine)

    if not tickers:
        print("No stock tickers found.")
        return 0

    count = 0

    with engine.begin() as conn:

        for ticker in tickers:
            try:
                price = get_close_price(ticker)

                # UPSERT price_history
                conn.execute(
                    text("""
                        INSERT INTO price_history (
                            ticker,
                            close_price,
                            price_date,
                            source
                        )
                        VALUES (
                            :ticker,
                            :close_price,
                            :price_date,
                            'yfinance'
                        )
                        ON CONFLICT (ticker, price_date)
                        DO UPDATE SET
                            close_price = EXCLUDED.close_price,
                            source = EXCLUDED.source,
                            created_at = now();
                    """),
                    price
                )

                # UPDATE portfolio
                conn.execute(
                    text("""
                        UPDATE portfolio
                        SET
                            market_price = :close_price,
                            price_date   = :price_date
                        WHERE ticker = :ticker
                    """),
                    price
                )

                count += 1

            except Exception as e:
                print(f"[WARN] {ticker}: {e}")

    return count
