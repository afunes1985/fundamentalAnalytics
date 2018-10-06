'''
Created on 18 sep. 2018

@author: afunes
'''

class Constant:

    CACHE_FOLDER = "C://Users//afunes//Downloads//cache//"

    DOCUMENT_SCH = "EX-101.SCH"
    DOCUMENT_INS = "EX-101.INS"
    DOCUMENT_SUMMARY = "FilingSummary.xml"
    DOCUMENT_PRE = "EX-101.PRE"
    
    PERIOD_DICT = "PERIOD_DICT"
    
    DOCUMENT_FISCAL_PERIOD_FOCUS = ['dei:DocumentFiscalPeriodFocus']
    DOCUMENT_FISCAL_YEAR_FOCUS =['dei:DocumentFiscalYearFocus']
    DOCUMENT_PERIOD_END_DATE =['dei:DocumentPeriodEndDate']
    XBRL_ROOT =['xbrli:xbrl','xbrl']
    XBRL_CONTEXT = ['xbrli:context','context']
    XBRL_PERIOD =['xbrli:period','period']
    XBRL_START_DATE =['xbrli:startDate','startDate']
    XBRL_END_DATE =['xbrli:endDate','endDate']
    XBRL_INSTANT =['xbrli:instant','instant']
    XBRL_ENTITY =['xbrli:entity','entity']
    XBRL_SEGMENT =['xbrli:segment','segment']
    LINKBASE =['link:linkbase','linkbase']
    PRESENTATON_LINK =["link:presentationLink","presentationLink"]
    LOC =["link:loc", "loc"]
    SCHEMA =["xs:schema", "xsd:schema", "schema"]
    ELEMENT =["xs:element", "xsd:element","element"]
    UNIT =["xbrli:unit","unit"]
    MEASURE =["xbrli:measure","measure"]
    PRESENTATIONARC =["link:presentationArc", "presentationArc"]
    CONTEXT_REF =["@contextRef"]
    
    LOGGER_ERROR = "Error"
    LOGGER_NONEFACTVALUE = "NoneFactValue"
    LOGGER_GENERAL = "general"
    LOGGER_ADDTODB = "addToDB"
    LOGGER_IMPORT_GENERAL = "importGeneral"