import datetime as dt


from src.get_historical_klines import klineData, generate_dates
from cred import cr

bb_sess = cr.bybit_session()

# Get today's date
today = dt.datetime.now() + dt.timedelta(days=1)

# Calculate the date 6 months ago
six_months_ago = (today - dt.timedelta(days=10)).strftime("%Y-%m-%d")
today_str = today.strftime("%Y-%m-%d")

date_list = [x for x in generate_dates(six_months_ago, today_str, hour_chuncks=16)]

symbol_list = [
    x["symbol"] for x in bb_sess.get_tickers(category="linear").get("result")["list"]
]

kl = klineData(
    symbol_list=symbol_list,
    interval="1",
    extract_dates=date_list,
    category="linear",
    num_threads=50,
)
data = kl.run_threads()
print(data)
