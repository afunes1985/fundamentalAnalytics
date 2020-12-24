'''
Created on Jun 26, 2020

@author: afunes
'''

from valueobject.constant import Constant


class ConstantStatus():
    
    FILE_STATUS = 'fileStatus'
    COMPANY_STATUS = 'companyStatus'
    ENTITY_FACT_STATUS = 'entityStatus'
    PRICE_STATUS = 'priceStatus'
    FACT_STATUS = 'factStatus'
    COPY_STATUS = 'copyStatus'
    CALCULATE_STATUS = 'calculateStatus'
    EXPRESSION_STATUS = 'expressionStatus'
    
    @staticmethod
    def getStatusDict():
        from importer.importerCalculate import ImporterCalculate
        from importer.importerCompany import ImporterCompany
        from importer.importerCopy import ImporterCopy
        from importer.importerEntityFact import ImporterEntityFact
        from importer.importerExpression import ImporterExpression
        from importer.importerFact import ImporterFact
        from importer.importerFile import ImporterFile
        from importer.importerPrice import ImporterPrice
        STATUS_DICT = {ConstantStatus.FILE_STATUS : {'importerClass':ImporterFile, 'status':ConstantStatus.FILE_STATUS, 'errorKey': Constant.ERROR_KEY_FILE, 'prevStatus' : None}, 
                        ConstantStatus.COMPANY_STATUS  : {'importerClass':ImporterCompany, 'status':ConstantStatus.COMPANY_STATUS, 'errorKey': Constant.ERROR_KEY_COMPANY, 'prevStatus' : ConstantStatus.FILE_STATUS}, 
                        ConstantStatus.ENTITY_FACT_STATUS  : {'importerClass':ImporterEntityFact, 'status':ConstantStatus.ENTITY_FACT_STATUS, 'errorKey': Constant.ERROR_KEY_ENTITY_FACT, 'prevStatus' : ConstantStatus.COMPANY_STATUS},
                        ConstantStatus.PRICE_STATUS  : {'importerClass':ImporterPrice, 'status':ConstantStatus.PRICE_STATUS, 'errorKey': Constant.ERROR_KEY_PRICE, 'prevStatus' : ConstantStatus.ENTITY_FACT_STATUS},
                        ConstantStatus.FACT_STATUS  : {'importerClass':ImporterFact, 'status':ConstantStatus.FACT_STATUS, 'errorKey': Constant.ERROR_KEY_FACT, 'prevStatus' : ConstantStatus.PRICE_STATUS},
                        ConstantStatus.COPY_STATUS  : {'importerClass':ImporterCopy, 'status':ConstantStatus.COPY_STATUS, 'errorKey': Constant.ERROR_KEY_COPY, 'prevStatus' : ConstantStatus.FACT_STATUS},
                        ConstantStatus.CALCULATE_STATUS  : {'importerClass':ImporterCalculate, 'status':ConstantStatus.CALCULATE_STATUS, 'errorKey': Constant.ERROR_KEY_CALCULATE, 'prevStatus' : ConstantStatus.COPY_STATUS},
                        ConstantStatus.EXPRESSION_STATUS  : {'importerClass':ImporterExpression, 'status':ConstantStatus.EXPRESSION_STATUS, 'errorKey': Constant.ERROR_KEY_EXPRESSION, 'prevStatus' : ConstantStatus.CALCULATE_STATUS} 
                    }
        return STATUS_DICT
