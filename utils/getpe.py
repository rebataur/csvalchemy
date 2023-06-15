import requests as r
import time

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
  
with open("C:\\3Projects\\downloads\\GROWWSCRIPCODE.csv","r") as f:
    next(f)
    for line in f.readlines():
        vals = line.split(',')
        code = vals[1]

        if code:            
            res = r.get(f'https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w?quotetype=EQ&scripcode={code}',headers=headers)           
            pe = res.json()['PE']
            print(pe)
            time.sleep(3)


            