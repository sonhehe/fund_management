# daily_job.py

from scripts.db_engine import get_engine
from scripts.update_prices import update_market_price
from scripts.portfolio import update_portfolio
from scripts.db import run_nav_pipeline
from scripts.db import load_table

def run():
    engine = get_engine()

    print("Step 1: Updating market prices...")
    update_market_price(engine)

    print("Step 2: Updating portfolio...")
    update_portfolio(engine)

    print("Step 3: Running NAV pipeline...")
    logs, result, error = run_nav_pipeline(engine)

    if error:
        raise Exception(error)

    print("Daily process completed successfully.")

if __name__ == "__main__":
    run()
