import yfinance as yf
from sqlalchemy import text




# ===== 1. LẤY GIÁ 1 MÃ =====
def get_close_price(ticker: str) -> dict:
    yf_ticker = ticker.upper() + ".VN"
    data = yf.Ticker(yf_ticker).history(period="5d")


    if data.empty:
        raise ValueError(f"No data for {ticker}")


    last = data.iloc[-1]


    return {
        "ticker": ticker.upper(),
        "close_price": float(last["Close"]),
        "price_date": last.name.date(),
    }




# ===== 2. UPDATE 1 MÃ =====
from sqlalchemy import text


def update_one_price(engine, ticker: str, source="yfinance"):
    price = get_close_price(ticker)
    with engine.begin() as conn:
        # 1️⃣ upsert price_history
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
                    :source
                )
                ON CONFLICT (ticker, price_date)
                DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    source = EXCLUDED.source,
                    created_at = now();
            """),
            {
                "ticker": price["ticker"],
                "close_price": price["close_price"],
                "price_date": price["price_date"],
                "source": source
            }
        )

        # 2️⃣ update portfolio price + price_date
        conn.execute(
            text("""
                UPDATE portfolio
                SET
                    market_price = :close_price,
                    price_date   = :price_date
                WHERE ticker = :ticker;
            """),
            {
                "ticker": price["ticker"],
                "close_price": price["close_price"],
                "price_date": price["price_date"]
            }
        )

# ===== 3. UPDATE TOÀN BỘ =====
def update_all_prices(engine, tickers: list[str]) -> int:
    count = 0


    for t in tickers:
        try:
            update_one_price(engine, t)
            count += 1
        except Exception as e:
            print(f"[WARN] {t}: {e}")


    return count



