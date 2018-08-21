
from engine.afratioengine import AFRatioEngine


#AFRatioEngine.calculateBookValue("320193")
#1318605
#320193
# from iexfinance import Stock
# tsla = Stock('TSLA')
# print(tsla.get_open())
# print(tsla.get_price())
# 
# from iexfinance import get_historical_data
# from datetime import datetime
# 
# start = datetime(2017, 2, 9)
# end = datetime(2017, 5, 24)
# 
# df = get_historical_data("AAPL", start=start, end=end, output_format='pandas')
# print(df.head())
# 
# import matplotlib.pyplot as plt
# 
# df.plot()
# plt.show()
from iexfinance import get_available_symbols

print(get_available_symbols(output_format='json')[:2])
