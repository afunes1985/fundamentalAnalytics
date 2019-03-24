from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from engine.customFactEngine import CustomFactEngine


Initializer()
session = DBConnector().getNewSession()

customConceptDict = {"REVENUE": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
                     "COST_OF_REVENUE": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
                     "GROSS_PROFIT": ["GrossProfit"],
                     "SELLING_AND_MARKETING_EXPENSE": ["SellingAndMarketingExpense"],
                     "GENERAL_AND_ADMINSTRATIVE_EXPENSE": ["GeneralAndAdministrativeExpense"],
                     "RESEARCH_AND_DEVELOPMENT_EXPENSE": ["ResearchAndDevelopmentExpense"],
                     "DEPRECIATION_AMORTIZATION_AND_OTHER": ["DepreciationAmortizationAndOther"],
                     "OPERATING_INCOME_LOSS": ["OperatingIncomeLoss"],
                     "OTHER_INCOME_LOSS": ["NonoperatingIncomeExpense"],
                     "INCOME_BEFORE_TAX": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
                     "INCOME_TAX_PAID": ["IncomeTaxExpenseBenefit"],
                     "NET_INCOME": ["NetIncomeLoss"],
                     "EARNINGS_PER_SHARE_BASIC": ["EarningsPerShareBasic"],
                     "EARNINGS_PER_SHARE_DILUTED": ["EarningsPerShareDiluted"]}

for customConcept, conceptList in customConceptDict.items():
    customConcept = Dao.getCustomConcept(customConcept, session)
    for conceptName in conceptList:
        concept = Dao.getConcept(conceptName, session)
        customConcept.conceptList.append(concept)
    Dao.addObject(objectToAdd = customConcept, session = session, doCommit = True)

