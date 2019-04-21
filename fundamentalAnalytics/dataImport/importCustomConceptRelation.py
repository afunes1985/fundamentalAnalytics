from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from engine.customFactEngine import CustomFactEngine


Initializer()
session = DBConnector().getNewSession()

createCustomConcept = True

if(createCustomConcept):
    ccList = []
    ccList.append(CustomFactEngine.createCustomConcept("REVENUE", "CUSTOM_INCOME", 1, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("COST_OF_REVENUE", "CUSTOM_INCOME", 2, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("GROSS_PROFIT", "CUSTOM_INCOME", 3, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("SELLING_AND_MARKETING_EXPENSE", "CUSTOM_INCOME", 4.1, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("GENERAL_AND_ADMINISTRATIVE_EXPENSE", "CUSTOM_INCOME", 4.2, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("SGA_EXPENSE", "CUSTOM_INCOME", 4, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("RESEARCH_AND_DEVELOPMENT_EXPENSE", "CUSTOM_INCOME", 5, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("DEPRECIATION", "CUSTOM_INCOME", 6, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("OPERATING_INCOME", "CUSTOM_INCOME", 7, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("INTERES_EXPENSE", "CUSTOM_INCOME", 8, 'QTD', session));
    ccList.append(CustomFactEngine.createCustomConcept("GAIN_LOSS_SALE_ASSETS", "CUSTOM_INCOME", 9, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("OTHER_INCOME_LOSS", "CUSTOM_INCOME", 10, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("INCOME_BEFORE_TAX", "CUSTOM_INCOME", 11, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("INCOME_TAX_PAID", "CUSTOM_INCOME", 12, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("NET_INCOME", "CUSTOM_INCOME", 13, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("EARNINGS_PER_SHARE_BASIC", "CUSTOM_INCOME", 14, 'QTD',session));
    ccList.append(CustomFactEngine.createCustomConcept("EARNINGS_PER_SHARE_DILUTED", "CUSTOM_INCOME", 15, 'QTD',session));
    
    ccList.append(CustomFactEngine.createCustomConcept("CASH_AND_CASH_EQUIVALENTS", "CUSTOM_BALANCE", 1, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("INVENTORY", "CUSTOM_BALANCE", 2, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("ACCOUNTS_RECEIVABLE", "CUSTOM_BALANCE", 3, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("PREPAID_EXPENSE", "CUSTOM_BALANCE", 4, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("OTHER_CURRENT_ASSETS", "CUSTOM_BALANCE", 5, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("TOTAL_CURRENT_ASSETS", "CUSTOM_BALANCE", 6, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("PROPERTY_PLANT_EQUIPMENT", "CUSTOM_BALANCE", 7, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("GOODWILL", "CUSTOM_BALANCE", 8, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("INTANGIBLES", "CUSTOM_BALANCE", 9, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("LONG_TERM_INVESTMENTS", "CUSTOM_BALANCE", 10, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("OTHER_LONG_TERM_INVESTMENTS", "CUSTOM_BALANCE", 11, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("TOTAL_ASSETS", "CUSTOM_BALANCE", 12, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("ACCOUNTS_PAYABLE", "CUSTOM_BALANCE", 13, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("ACCRUED_EXPENSES", "CUSTOM_BALANCE", 14, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("LONG_TERM_DEBT_CURRENT", "CUSTOM_BALANCE", 16, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("OTHER_CURRENT_LIABILITIES", "CUSTOM_BALANCE", 17, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("TOTAL_CURRENT_LIABILITIES", "CUSTOM_BALANCE", 18, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("LONG_TERM_DEBT", "CUSTOM_BALANCE", 19, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("DEFERRED_INCOME_TAXES", "CUSTOM_BALANCE", 20, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("OTHER_LIABILITIES", "CUSTOM_BALANCE", 22, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("TOTAL_LIABILITIES", "CUSTOM_BALANCE", 23, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("PREFERRED_STOCK", "CUSTOM_BALANCE", 24, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("COMMON_STOCK", "CUSTOM_BALANCE", 25, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("ADDITIONAL_PAID_IN_CAPITAL", "CUSTOM_BALANCE", 26, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("RETAINED_EARNINGS", "CUSTOM_BALANCE", 27, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("THREASURY_STOCK", "CUSTOM_BALANCE", 28, 'INST', session));
    ccList.append(CustomFactEngine.createCustomConcept("TOTAL_SHAREHOLDERS_EQUITY", "CUSTOM_BALANCE", 29, 'INST', session));
    
    for itemToAdd in ccList:
        Dao.addObject(objectToAdd = itemToAdd, session = session, doCommit = True) 

customConceptDict = {"REVENUE": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
                     "COST_OF_REVENUE": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
                     "GROSS_PROFIT": ["GrossProfit"],
                     "SELLING_AND_MARKETING_EXPENSE": ["SellingAndMarketingExpense"],
                     "GENERAL_AND_ADMINISTRATIVE_EXPENSE": ["GeneralAndAdministrativeExpense"],
                     "SGA_EXPENSE": ["SellingGeneralAndAdministrativeExpense"],
                     "RESEARCH_AND_DEVELOPMENT_EXPENSE": ["ResearchAndDevelopmentExpense"],
                     "DEPRECIATION": ["DepreciationAmortizationAndOther", "DepreciationAndAmortization", "AmortizationofAcquisitionRelatedIntangibleAssets", "Depreciation"],
                     "OPERATING_INCOME": ["OperatingIncomeLoss"],
                     "INTERES_EXPENSE": ["InterestExpense"],
                     "GAIN_LOSS_SALE_ASSETS": ["GainLossOnInvestments"],
                     "OTHER_INCOME_LOSS": ["NonoperatingIncomeExpense", "OtherNonoperatingIncomeExpense"],
                     "INCOME_BEFORE_TAX": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"],
                     "INCOME_TAX_PAID": ["IncomeTaxExpenseBenefit"],
                     "NET_INCOME": ["NetIncomeLoss", "ProfitLoss"],
                     "EARNINGS_PER_SHARE_BASIC": ["EarningsPerShareBasic"],
                     "EARNINGS_PER_SHARE_DILUTED": ["EarningsPerShareDiluted"],
                     
                     "CASH_AND_CASH_EQUIVALENTS": ["CashAndCashEquivalentsAtCarryingValue"],
                     "INVENTORY": ["InventoryNet"],
                     "ACCOUNTS_RECEIVABLE": ["AccountsReceivableNetCurrent"],
                     "PREPAID_EXPENSE": ["PrepaidExpenseAndOtherAssetsCurrent"],
                     "OTHER_CURRENT_ASSETS": ["OtherAssetsCurrent"],
                     "TOTAL_CURRENT_ASSETS": ["AssetsCurrent"],
                     "PROPERTY_PLANT_EQUIPMENT": ["PropertyPlantAndEquipmentNet"],
                     "GOODWILL": ["Goodwill"],
                     "INTANGIBLES": ["FiniteLivedIntangibleAssetsNet"],
                     "LONG_TERM_INVESTMENTS": ["LongTermInvestments"],
                     "OTHER_LONG_TERM_INVESTMENTS": ["OtherAssetsNoncurrent"],
                     "TOTAL_ASSETS": ["Assets"],
                     "ACCOUNTS_PAYABLE": ["AccountsPayableCurrent"],
                     "ACCRUED_EXPENSES": ["AccruedLiabilitiesCurrent"],
                     "OTHER_CURRENT_LIABILITIES": ["OtherLiabilitiesCurrent"],
                     "LONG_TERM_DEBT_CURRENT": ["LongTermDebtCurrent"],
                     "TOTAL_CURRENT_LIABILITIES": ["LiabilitiesCurrent"],
                     "LONG_TERM_DEBT": ["LongTermDebtNoncurrent"],
                     "DEFERRED_INCOME_TAXES": ["DeferredTaxLiabilitiesNoncurrent"],
                     "OTHER_LIABILITIES": ["OtherLiabilitiesNoncurrent"],
                     "TOTAL_LIABILITIES": ["Liabilities"],
                     "PREFERRED_STOCK": ["PreferredStockValue"],
                     "COMMON_STOCK": ["CommonStocksIncludingAdditionalPaidInCapital", "CommonStockValue"],
                     "ADDITIONAL_PAID_IN_CAPITAL": ["AdditionalPaidInCapital", "AdditionalPaidInCapitalCommonStock"],
                     "RETAINED_EARNINGS": ["RetainedEarningsAccumulatedDeficit"],
                     "THREASURY_STOCK": ["TreasuryStockValue"],
                     "TOTAL_SHAREHOLDERS_EQUITY": ["StockholdersEquity"]}

for customConceptName, conceptList in customConceptDict.items():
    print ("Configuring " + customConceptName)
    customConcept = Dao.getCustomConcept(customConceptName, session)
    customConcept.conceptList = []
    for conceptName in conceptList:
        concept = Dao.getConcept(conceptName, session)
        print("    Concept added " +  concept.conceptName)
        customConcept.conceptList.append(concept)
    Dao.addObject(objectToAdd = customConcept, session = session, doCommit = True)

