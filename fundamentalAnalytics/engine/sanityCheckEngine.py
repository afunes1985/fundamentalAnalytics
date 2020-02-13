'''
Created on Jun 3, 2019

@author: afunes
'''
class SanityCheckEngine():
    pass

#borra duplicados periodos huerfanos    
# delete from fa_period where oid in(
# select c.oid from (
# select p.oid
#     from fa_period p
#         left join fa_fact_value fv on fv.periodOID = p.OID
#         left join fa_entity_fact_value efv on efv.periodOID = p.OID
#         left join fa_custom_fact_value cfv on cfv.periodOID = p.OID
#         left join fa_price pri on pri.periodOID = p.OID
# where fv.OID is null
#     and efv.OID is null
#     and cfv.oid is null
#     and pri.oid is null)as c);

#busca duplicados en periodos
# select endDate
#         from fa_period 
#     where startDate is null 
#         and instant is null
#     group by endDate
#     having count(*)>1

#borra periodos duplicados
# update fa_custom_fact_value a   
#         join (
#             select max(p.oid) as maxOID, min(p.oid) as minOID
#                         from fa_period p 
#                         #join fa_custom_fact_value t on t.periodOID = p.OID
#                     where p.startDate is null 
#                         and p.instant is null
#                     group by p.endDate
#                     having count(*)>1) as z on z.maxOID = a.periodOID  
# set a.periodOID = minOID; 

#busca periodos sin type
#select * from fa_period where type is null;

#borra los fact value lejos de la fecha del documento
# delete from fa_fact_value where oid in(
# select c.OID from(
# select factValue.OID
#                                  FROM fa_fact fact
#                                      join fa_file_data fd on fd.OID = fact.fileDataOID
#                                      #join fa_company company on fd.companyOID = company.OID
#                                      #join fa_concept concept on fact.conceptOID = concept.OID
#                                      #join fa_report report on fact.reportOID = report.OID
#                                      join fa_fact_value factValue on factValue.factOID = fact.OID
#                                      join fa_period period on factValue.periodOID = period.OID
#                                  where 
#                                      period.type = 'INST' 
#                                      and ABS(DATEDIFF(period.instant, fd.documentPeriodEndDate))>90) as c);
#                                      
#                                      
# busca los fact value lejos de la fecha del documento
# select ABS(DATEDIFF(period.instant, fd.documentPeriodEndDate)), period.instant, fd.documentPeriodEndDate
#                                  FROM fa_fact fact
#                                      join fa_file_data fd on fd.OID = fact.fileDataOID
#                                      join fa_company company on fd.companyOID = company.OID
#                                      #join fa_concept concept on fact.conceptOID = concept.OID
#                                      #join fa_report report on fact.reportOID = report.OID
#                                      join fa_fact_value factValue on factValue.factOID = fact.OID
#                                      join fa_period period on factValue.periodOID = period.OID
#                                  where 
#                                      period.type = 'INST' 
#                                      and ABS(DATEDIFF(period.instant, fd.documentPeriodEndDate))>90
#borra fact value huerfanos
# delete  fa_fact_value
#     
#     from fa_fact_value inner join (
#         select c.OID from(
#             select fv.OID
#         from fa_fact_value fv 
#         left join fa_fact f on f.factValueOID = fv.OID 
#     where f.OID is null) as c) as d on d.OID = fa_fact_value.OID;

