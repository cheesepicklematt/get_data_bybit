import json
import os
from pybit.unified_trading import HTTP


class cred:
    def __init__(self) -> None:
        self.bybit_cred_path = os.path.join(os.path.expanduser("~"),"0_cred","bybitAPI.txt")

    def bybit_session(self):
        creds = self.readDictFromTxt(self.bybit_cred_path)
        key = creds['bybitAPI']
        secret = creds['bybitAPISecret']
        session = HTTP(api_key=key, api_secret=secret, testnet=False)
        return session


    @staticmethod
    def readDictFromTxt(path):
        # reading the data from the file
        with open(path) as f:
            data = f.read()
        return json.loads(data)

cr = cred()