import pandas as pd
import numpy as np
import yfinance as yf
spy_ohlc_df = yf.download('SPY', start='1993-02-01', end='2023-06-16')
with open('C:\\3Projects\\downloads\\SPY.csv','w') as f:
    f.write(spy_ohlc_df.to_csv())