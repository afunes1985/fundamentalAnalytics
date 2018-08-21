'''
Created on 20 ago. 2018

@author: afunes
'''
class DaoCompanyFundamental():
    @staticmethod
    def insertCompanyFundamental(cf):
        insertSentence = """insert company_fundamental(company_id, indicator_id,
                            2011Q2,2011Q3,2011Q4,2012Q1,2012Q2,2012Q3,2012Q4,
                            2013Q1,2013Q2,2013Q3,2013Q4,2014Q1,2014Q2,2014Q3,2014Q4,
                            2015Q1,2015Q2,2015Q3,2015Q4,2016Q1,2016Q2,2016Q3,2016Q4,
                            2017Q1,2017Q2,2017Q3,2017Q4,2018Q1,2018Q2) 
                       values (%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s)"""
        return DbConnector().doInsert(insertSentence, (cf.q_company_id, cf.q_indicator_id,cf.q_2011Q2,cf.q_2011Q3,cf.q_2011Q4,cf.q_2012Q1,cf.q_2012Q2,cf.q_2012Q3,cf.q_2012Q4,cf.q_2013Q1,cf.q_2013Q2,cf.q_2013Q3,cf.q_2013Q4,cf.q_2014Q1,cf.q_2014Q2,cf.q_2014Q3,cf.q_2014Q4,cf.q_2015Q1,cf.q_2015Q2,cf.q_2015Q3,cf.q_2015Q4,cf.q_2016Q1,cf.q_2016Q2,cf.q_2016Q3,cf.q_2016Q4,cf.q_2017Q1,cf.q_2017Q2,cf.q_2017Q3,cf.q_2017Q4,cf.q_2018Q1,cf.q_2018Q2))
    
    @staticmethod
    def getCompanyFundamental(company_id, indicator_id):
        query = """SELECT 2011Q2,2011Q3,2011Q4,2012Q1,2012Q2,2012Q3,2012Q4,
                            2013Q1,2013Q2,2013Q3,2013Q4,2014Q1,2014Q2,2014Q3,2014Q4,
                            2015Q1,2015Q2,2015Q3,2015Q4,2016Q1,2016Q2,2016Q3,2016Q4,
                            2017Q1,2017Q2,2017Q3,2017Q4,2018Q1,2018Q2
                    FROM company_fundamental cf
                WHERE cf.company_id = %s
                    AND cf.indicator_id = %s"""
        resultDict = DbConnector().doQuery2(query, (company_id, indicator_id))
        return resultDict 
    
    @staticmethod
    def getCompanyFundamental2(company_id, indicator_id):
        paramns = {'company_id' : company_id,
                   'indicator_id': indicator_id}
        query = """SELECT c.name, i.section, cf.indicator_id, i.label,2011Q2,2011Q3,2011Q4,2012Q1,2012Q2,2012Q3,2012Q4,
                            2013Q1,2013Q2,2013Q3,2013Q4,2014Q1,2014Q2,2014Q3,2014Q4,
                            2015Q1,2015Q2,2015Q3,2015Q4,2016Q1,2016Q2,2016Q3,2016Q4,
                            2017Q1,2017Q2,2017Q3,2017Q4,2018Q1,2018Q2
                    FROM company c
                        inner join company_fundamental cf on c.company_id = cf.company_id
                        left join fa_concept i on i.indicator_id = cf.indicator_id
                WHERE c.company_id = %(company_id)s
                    AND (cf.indicator_id = %(indicator_id)s or %(indicator_id)s is null)
                order by  i.section, cf.indicator_id"""
        resultDict = DbConnector().doQuery2(query, paramns)
        return resultDict 