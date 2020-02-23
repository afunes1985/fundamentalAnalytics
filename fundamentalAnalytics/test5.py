from datetime import timedelta

import requests

webSession = requests.Session()
webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
webSession.trust_env = False
priceList = []
ticker =  'PTIX'
dateToImport = '2018-02-28'
dateToImport1 = '2018-01-01'
if dateToImport is not None:
    if(ticker is None):
        raise Exception("Ticker not found")
    url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + ticker +'&interval=daily&start='+dateToImport1+ '&end=' + dateToImport
    response = webSession.get(url, timeout=2)
    r = response.json()
    if(r["history"] is not None and r["history"]["day"] is not None):
        print(  r["history"]["day"][len(r["history"]["day"])-1]["close"])
