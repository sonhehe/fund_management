import yfinance as yf
from sqlalchemy import text
import datetime

# ==============================
# 1️⃣ LẤY GIÁ
# ==============================
def get_close_price(ticker: str) -> dict:
    ticker_clean = ticker.upper().replace(".VN", "")
    yf_ticker = f"{ticker_clean}.VN"

    data = yf.Ticker(yf_ticker).history(period="1mo", auto_adjust=False)

    if data.empty:
        raise ValueError(f"No data for {ticker_clean}")

    data = data[["Close"]].dropna()

    if data.empty:
        raise ValueError(f"No valid close price for {ticker_clean}")

    last_idx = data.index[-1]
    last_close = float(data.iloc[-1]["Close"])

    market_date = last_idx.date()
    updated_at = datetime.date.today()

    return {
        "ticker": ticker_clean,
        "close_price": last_close,
        "market_date": market_date,
        "updated_at": updated_at
    }

# ==============================
# 2️⃣ LẤY DANH SÁCH STOCK
# ==============================
def get_stock_tickers(engine):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT DISTINCT UPPER(ticker) AS ticker
                FROM portfolio
                WHERE asset_type = 'Stock'
                  AND ticker IS NOT NULL
                  AND TRIM(ticker) <> ''
            """)
        )
        return [row[0] for row in result]

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

                # Lưu lịch sử giá theo ngày thị trường
                conn.execute(
                    text("""
                        INSERT INTO price_history (
                            ticker,
                            close_price,
                            price_date,
                            source,
                            created_at
                        )
                        VALUES (
                            :ticker,
                            :close_price,
                            :market_date,
                            'yfinance',
                            now()
                        )
                        ON CONFLICT (ticker, price_date)
                        DO UPDATE SET
                            close_price = EXCLUDED.close_price,
                            source = EXCLUDED.source,
                            created_at = now();
                    """),
                    price
                )

                # Update portfolio theo ngày hệ thống
                conn.execute(
                    text("""
                        UPDATE portfolio
                        SET
                            market_price = :close_price,
                            price_date   = :updated_at
                        WHERE UPPER(ticker) = :ticker
                          AND asset_type = 'Stock';
                    """),
                    price
                )

                count += 1

            except Exception as e:
                print(f"[WARN] {ticker}: {e}")

    return count
