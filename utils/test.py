import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

def get_company_ratios(code):  
    time.sleep(2)
    

    URL = f"https://www.screener.in/company/{code}"
   
    
    try:
        table_MN = pd.read_html(URL)
       
        # print(table_MN[6])
        # borrowing_prev = float(table_MN[6].iloc[:,-2:].iloc[2][0])
        print(table_MN[6].iloc[:,-2:])
        borrowing_now = float(table_MN[6].iloc[:,-2:].iloc[2][1])
        print(borrowing_now)
        reserves_now = float(table_MN[6].iloc[:,-2:].iloc[1][1])
        print(reserves_now)
        share_capital = float(table_MN[6].iloc[:,-2:].iloc[0][1])
        print(share_capital)

        de_ratio = borrowing_now/(reserves_now+share_capital)
        return round(de_ratio,2)

        
    except Exception as e:
        print("Error in getting ratio")
        print(e)
        return -1



if __name__ == '__main__':
    val =  get_company_ratios(543428)
    print(val)