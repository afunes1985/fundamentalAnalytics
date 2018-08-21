

from dao.dao import DaoCompanyFundamental
import matplotlib.pyplot as plt

class utils():
    @staticmethod
    def getListY(resultDict):
        listY = []
        if (len(resultDict["rs"]) != 0): 
            for row in resultDict["rs"][0]:
                if row is not None:
                    listY.append(row/1000000)
                else:
                    listY.append(row)
        return listY
    
    @staticmethod        
    def getListX(resultDict):
        listX = []
        if (len(resultDict["column"]) != 0): 
            for row in resultDict["column"]:
                if row is not None:
                    listX.append(row[2:6])
        return listX

indicatorID = 'RetainedEarningsAccumulatedDeficit'
resultDict = DaoCompanyFundamental.getCompanyFundamental('50863', indicatorID)
resultDict2 = DaoCompanyFundamental.getCompanyFundamental('2488', indicatorID)
print(resultDict["column"])
print(resultDict["rs"])
listY = utils.getListY(resultDict)
listX = utils.getListX(resultDict)
plt.plot(listX, listY, color='orange')
listY2 = utils.getListY(resultDict2)
listX2 = utils.getListX(resultDict2)
plt.plot(listX2, listY2, color='green')
plt.xlabel('Quarter')
plt.ylabel('Millions')
plt.title(indicatorID)
plt.show() 

