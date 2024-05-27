import datetime as dt
import pandas as pd
import pytz
import random
import time
import queue
import threading
import sys

sys.path.insert(0, "./")

from ..cred import cr

bb_sess = cr.bybit_session()


def generate_dates(start_date_str, end_date_str, hour_chuncks=5):
    start_date = dt.datetime.strptime(start_date_str, "%Y-%m-%d").replace(
        tzinfo=pytz.utc
    )
    end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=pytz.utc)

    dates_list = []
    current_date = start_date

    while current_date <= end_date:
        dates_list.append(int(current_date.timestamp() * 1000))
        current_date += dt.timedelta(hours=hour_chuncks)

    return dates_list


class klineData:
    """
    NOTE:
        1. max page size is 1000 rows - ensure that datapoints between start and end times are < 1000 rows
            - data points between extract dates needs to be < 1000 rows
            -  e.g. 1000/60 (minutes in an hour) = 16.66, therefore can use a maximum of 16 hour time intervals in extract_dates for 1 minute data

    TODO
        1. build exceptions where API call fails
    """

    def __init__(
        self,
        symbol_list,
        interval,
        extract_dates,
        category,
        num_threads,
        num_rows=1000,
        verbose=True,
    ) -> None:
        self.symbol_list = symbol_list
        self.interval = interval
        self.extract_dates = extract_dates
        self.category = category
        self.num_threads = num_threads
        self.max_retries = 10
        self.num_rows = num_rows
        self.verbose = verbose

        self.bb_sess = cr.bybit_session()

    def run_threads(self):
        random.shuffle(self.symbol_list)

        thread_symbol_lists = self._split_list(self.symbol_list, self.num_threads)

        result_queue = queue.Queue()
        threads = []
        for symbol_list in thread_symbol_lists:
            thread = threading.Thread(
                target=self.execute_subset,
                kwargs={"extract_list": symbol_list, "data_queue": result_queue},
            )
            threads.append(thread)
            thread.start()
            time.sleep(0.2)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        res_kline_dfs = []
        while not result_queue.empty():
            result = result_queue.get()
            if not result.empty:
                res_kline_dfs.append(result)

        final_data = self._merge_data(df_list=res_kline_dfs)
        return final_data

    def _try_extract(self, symbol):
        retries = 0
        while retries < self.max_retries:
            try:
                if len(self.extract_dates) == 1:
                    data = self._extract_klines(
                        symbol=symbol,
                        category=self.category,
                        start=self.extract_dates[0],
                        interval=self.interval,
                    )
                    symbol_data = data
                else:
                    symbol_data = []
                    for di in range(1, len(self.extract_dates)):
                        start = self.extract_dates[di - 1]
                        end = self.extract_dates[di]

                        data = self._extract_klines(
                            symbol=symbol,
                            category=self.category,
                            start=start,
                            end=end,
                            interval=self.interval,
                        )
                        if not data.empty:
                            symbol_data.append(data)

                    if len(symbol_data) > 0:
                        symbol_data = pd.concat(symbol_data).drop_duplicates()
                return symbol_data
            except Exception as e:
                retries += 1
                print(
                    f"Attempt {retries} failed for {symbol}. Exception: {str(e)[0:100]}"
                )
                time.sleep(30)
                if retries >= self.max_retries:
                    print(f"Max retries reached for {symbol}. Data NOT extracted.")

    def execute_subset(self, extract_list, data_queue):
        all_data = []
        for symbol in extract_list:
            symbol_data = self._try_extract(symbol=symbol)
            if len(symbol_data) > 0:
                all_data.append(symbol_data)
                if self.verbose:
                    print(f"{symbol} data extracted ")
            else:
                print(f"no data for {symbol}")

        merged_data = self._merge_data(df_list=all_data)
        data_queue.put(merged_data)

    def _extract_klines(
        self, symbol: str, category: str, start: int, interval: str, end: int = None
    ):
        if end is None:
            response = self.bb_sess.get_kline(
                category=category,
                symbol=symbol,
                start=start,
                interval=interval,
                limit=self.num_rows,
            ).get("result")
        else:
            response = self.bb_sess.get_kline(
                category=category,
                symbol=symbol,
                start=start,
                end=end,
                interval=interval,
                limit=self.num_rows,
            ).get("result")

        data = pd.DataFrame(
            response["list"],
            columns=[
                "Open time",
                f"Open_{symbol}",
                f"High_{symbol}",
                f"Low_{symbol}",
                f"Close_{symbol}",
                f"Asset volume_{symbol}",
                f"Volume_{symbol}",
            ],
        )

        data["Open time formatted UTC"] = pd.to_datetime(
            data["Open time"].astype(int), unit="ms"
        )
        data = data.sort_values(by="Open time formatted UTC").reset_index(drop=True)
        return data

    @staticmethod
    def _merge_data(df_list):
        date_col = "Open time formatted UTC"
        merged_data = pd.DataFrame(
            list(set([a for b in [x[date_col].unique() for x in df_list] for a in b])),
            columns=[date_col],
        ).sort_values(by=date_col)

        for data in df_list:
            try:
                del data["Open time"]
            except:
                pass
            merged_data = merged_data.merge(data, on=date_col, how="left")
        return merged_data

    @staticmethod
    def _split_list(lst, x):
        """Split original_list into x smaller lists as evenly as possible."""
        # Calculate the size of each split
        k, m = divmod(len(lst), x)
        return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(x)]
