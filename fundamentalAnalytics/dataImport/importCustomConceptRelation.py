from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from engine.customFactEngine import CustomFactEngine


Initializer()
session = DBConnector().getNewSession()

createCustomConcept = True

if(createCustomConcept): 
    ccList = []
    customFactEngine = CustomFactEngine()
    ccList.append(customFactEngine.createCustomConcept("REVENUE", "CUSTOM_INCOME", 1, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("COST_OF_REVENUE", "CUSTOM_INCOME", 2, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("GROSS_PROFIT", "CUSTOM_INCOME", 3, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("SELLING_AND_MARKETING_EXPENSE", "CUSTOM_INCOME", 4.1, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("GENERAL_AND_ADMINISTRATIVE_EXPENSE", "CUSTOM_INCOME", 4.2, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("SGA_EXPENSE", "CUSTOM_INCOME", 4, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("RESEARCH_AND_DEVELOPMENT_EXPENSE", "CUSTOM_INCOME", 5, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("DEPRECIATION", "CUSTOM_INCOME", 6, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OPERATING_INCOME", "CUSTOM_INCOME", 7, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("INTERES_EXPENSE", "CUSTOM_INCOME", 8, 'QTD', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("GAIN_LOSS_SALE_ASSETS", "CUSTOM_INCOME", 9, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OTHER_INCOME_LOSS", "CUSTOM_INCOME", 10, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("INCOME_BEFORE_TAX", "CUSTOM_INCOME", 11, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("INCOME_TAX_PAID", "CUSTOM_INCOME", 12, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("NET_INCOME", "CUSTOM_INCOME", 13, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("EARNINGS_PER_SHARE_BASIC", "CUSTOM_INCOME", 14, 'QTD',"COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("EARNINGS_PER_SHARE_DILUTED", "CUSTOM_INCOME", 15, 'QTD',"COPY_CALCULATE", session));
    
    ccList.append(customFactEngine.createCustomConcept("CASH_AND_CASH_EQUIVALENTS", "CUSTOM_BALANCE", 1, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("INVENTORY", "CUSTOM_BALANCE", 2, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("ACCOUNTS_RECEIVABLE", "CUSTOM_BALANCE", 3, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("PREPAID_EXPENSE", "CUSTOM_BALANCE", 4, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OTHER_CURRENT_ASSETS", "CUSTOM_BALANCE", 5, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("TOTAL_CURRENT_ASSETS", "CUSTOM_BALANCE", 6, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("PROPERTY_PLANT_EQUIPMENT", "CUSTOM_BALANCE", 7, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("GOODWILL", "CUSTOM_BALANCE", 8, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("INTANGIBLES", "CUSTOM_BALANCE", 9, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("LONG_TERM_INVESTMENTS", "CUSTOM_BALANCE", 10, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OTHER_LONG_TERM_INVESTMENTS", "CUSTOM_BALANCE", 11, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("TOTAL_ASSETS", "CUSTOM_BALANCE", 12, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("ACCOUNTS_PAYABLE", "CUSTOM_BALANCE", 13, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("ACCRUED_EXPENSES", "CUSTOM_BALANCE", 14, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("LONG_TERM_DEBT_CURRENT", "CUSTOM_BALANCE", 16, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OTHER_CURRENT_LIABILITIES", "CUSTOM_BALANCE", 17, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("TOTAL_CURRENT_LIABILITIES", "CUSTOM_BALANCE", 18, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("LONG_TERM_DEBT", "CUSTOM_BALANCE", 19, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("DEFERRED_INCOME_TAXES", "CUSTOM_BALANCE", 20, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("OTHER_LIABILITIES", "CUSTOM_BALANCE", 22, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("TOTAL_LIABILITIES", "CUSTOM_BALANCE", 23, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("PREFERRED_STOCK", "CUSTOM_BALANCE", 24, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("COMMON_STOCK", "CUSTOM_BALANCE", 25, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("ADDITIONAL_PAID_IN_CAPITAL", "CUSTOM_BALANCE", 26, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("RETAINED_EARNINGS", "CUSTOM_BALANCE", 27, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("THREASURY_STOCK", "CUSTOM_BALANCE", 28, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("TOTAL_SHAREHOLDERS_EQUITY", "CUSTOM_BALANCE", 29, 'INST', "COPY_CALCULATE", session));
    ccList.append(customFactEngine.createCustomConcept("SHARES_OUTSTANDING", "CUSTOM_BALANCE", 30, 'INST', "COPY_CALCULATE", session));
    
    ccList.append(customFactEngine.createCustomConcept("GROSS_PROFIT_MARGIN", "CUSTOM_RATIO", 1, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("SGA_MARGIN", "CUSTOM_RATIO", 2, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("RES_AND_DEV_EXPENSE_MARGIN", "CUSTOM_RATIO", 3, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("DEPRECIATION_MARGIN", "CUSTOM_RATIO", 4, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("NET_INCOME_MARGIN", "CUSTOM_RATIO", 5, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("INCOME_BEFORE_TAX_BY_SHARES", "CUSTOM_RATIO", 6, 'QTD', "EXPRESSION", session));
    ccList.append(customFactEngine.createCustomConcept("NET_INCOME_BY_SHARES", "CUSTOM_RATIO", 7, 'QTD', "EXPRESSION", session));
    
    for itemToAdd in ccList:
        Dao().addObject(objectToAdd = itemToAdd, session = session, doCommit = True) 

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
                     "TOTAL_SHAREHOLDERS_EQUITY": ["StockholdersEquity"],
                     "SHARES_OUTSTANDING": ["CommonStockSharesOutstanding"]}

for customConceptName, conceptList in customConceptDict.items():
    print ("Configuring " + customConceptName)
    customConcept = Dao.getCustomConcept(customConceptName, session)
    customConcept.conceptList = []
    for conceptName in conceptList:
        concept = Dao.getConcept(conceptName, session)
        print("    Concept added " +  concept.conceptName)
        customConcept.conceptList.append(concept)
    Dao().addObject(objectToAdd = customConcept, session = session, doCommit = True)

