from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from engine.customFactEngine import CustomFactEngine


Initializer()
session = DBConnector().getNewSession()

createCustomConcept = True

if(createCustomConcept):
    customConcept = CustomFactEngine.createCustomConcept("SGA_EXPENSE", "CUSTOM_INCOME", 4, session);
    Dao.addObject(objectToAdd = customConcept, session = session, doCommit = True) 

customConceptDict = {"REVENUE": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
                     "COST_OF_REVENUE": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
                     "GROSS_PROFIT": ["GrossProfit"],
                     "SELLING_AND_MARKETING_EXPENSE": ["SellingAndMarketingExpense"],
                     "GENERAL_AND_ADMINISTRATIVE_EXPENSE": ["GeneralAndAdministrativeExpense"],
                     "SGA_EXPENSE": ["SellingGeneralAndAdministrativeExpense"],
                     "RESEARCH_AND_DEVELOPMENT_EXPENSE": ["ResearchAndDevelopmentExpense"],
                     "DEPRECIATION_AMORTIZATION_AND_OTHER": ["DepreciationAmortizationAndOther", "DepreciationAndAmortization", "AmortizationofAcquisitionRelatedIntangibleAssets"],
                     "OPERATING_INCOME_LOSS": ["OperatingIncomeLoss"],
                     "OTHER_INCOME_LOSS": ["NonoperatingIncomeExpense"],
                     "INCOME_BEFORE_TAX": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
                     "INCOME_TAX_PAID": ["IncomeTaxExpenseBenefit"],
                     "NET_INCOME": ["NetIncomeLoss"],
                     "EARNINGS_PER_SHARE_BASIC": ["EarningsPerShareBasic"],
                     "EARNINGS_PER_SHARE_DILUTED": ["EarningsPerShareDiluted"]}

for customConcept, conceptList in customConceptDict.items():
    print ("Configuring " + customConcept)
    customConcept = Dao.getCustomConcept(customConcept, session)
    customConcept.conceptList = []
    for conceptName in conceptList:
        concept = Dao.getConcept(conceptName, session)
        print("    Concept added " +  concept.conceptName)
        customConcept.conceptList.append(concept)
    Dao.addObject(objectToAdd = customConcept, session = session, doCommit = True)

