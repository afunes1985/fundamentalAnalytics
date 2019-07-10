'''
Created on Jun 3, 2019

@author: afunes
'''
class SanityCheckEngine():
    pass
    
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