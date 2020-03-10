#udate listed company with shares = 0
#update fa_company set listed = 0, notListedDescription = 'sum EntityCommonStockSharesOutstanding = 0' 
where oid in (     
select a.oid from (SELECT c.oid
	FROM fa_entity_fact_value efv
		join fa_entity_fact ef on ef.oid = efv.entityFactOID
		join fa_file_data fd on fd.oid = efv.fileDataOID
        join fa_company	c on c.oid = fd.companyOID
        join fa_concept	co on co.oid = ef.conceptOID
    group by c.oid
	having sum(efv.value) = 0) as a);

update fa_file_data set entityStatus = 'PENDING' where companyStatus != 'OK';
update fa_file_data set priceStatus = 'PENDING' where entityStatus != 'OK';
update fa_file_data set factStatus = 'PENDING' where priceStatus != 'OK';
update fa_file_data set copyStatus = 'PENDING' where (factStatus != 'OK' and factStatus != 'NO_DATA');
update fa_file_data set calculateStatus = 'PENDING' where (copyStatus != 'OK' and copyStatus != 'NO_DATA');
update fa_file_data set expressionStatus = 'PENDING' where (calculateStatus != 'OK' and calculateStatus != 'NO_DATA');

#update fd where listed = 0
update fa_file_data set copyStatus = 'PENDING', calculateStatus = 'PENDING', expressionStatus = 'PENDING', factStatus = 'PENDING', entityStatus = 'PENDING', priceStatus = 'PENDING'
where oid in(  
select a.oid from(
select fd.OID 
	from fa_file_data fd 
    inner join fa_company	c on c.oid = fd.companyOID
    where c.listed = 0) a);

#delete copy, calculate and expression custom values where copyStatus = PENDING
delete from fa_custom_fact_value where oid in(
select a.oid from(
select CFV.OID 
	from fa_custom_fact_value cfv
    inner join fa_file_data fd on cfv.fileDataOID = fd.OID
    where fd.copyStatus = 'PENDING') a);

#delete fa_custom_fact orphans may not usefull
#delete from fa_custom_fact cf
#		left join fa_custom_fact_value cfv on cfv.customFactOID = cf.oid
#where cfv.oid is null;
#
#

#delete entity  where entityStatus = 'PENDING'  
delete from fa_entity_fact_value where oid in(
select t.oid from (
select efv.oid
	from fa_entity_fact_value efv
    join fa_file_data fd on fd.oid = efv.fileDataOID
    where fd.entityStatus = 'PENDING') as t);

#TODO delete orphan entity fact
#
#
#
# 
 
#delete prices  where priceStatus = 'PENDING'  
delete from fa_price where oid in(
select t.oid from (
select p.oid
	from fa_price p
    join fa_file_data fd on fd.oid = p.fileDataOID
    where fd.priceStatus = 'PENDING') as t);

#delete fa_fact_report_relation where factStatus = 'PENDING'
delete from fa_fact_report_relation where factOID in(    
select f.OID 
	from fa_file_data fd
		join fa_fact f on f.fileDataOID = fd.OID
	where factStatus = 'PENDING');


#delete fa_fact where factStatus = 'PENDING'
delete from fa_fact where OID in(    
	select a.OID from (
	select f.OID 
		from fa_file_data fd
			join fa_fact f on f.fileDataOID = fd.OID
		where factStatus = 'PENDING') as a);

#delete fa_fact_value orphans
delete from fa_fact_value where oid in(
select a.oid from(
select fv.OID
from fa_fact_value fv
left join fa_fact f on fv.oid = f.factValueOID
where  f.oid is null)as a);

#delete orphans fa_error_message for EXPRESSION_ERROR
delete from fa_error_message where fileDataOID in(
select a.fileDataOID from (
select em.fileDataOID
	from fa_error_message em
		inner join fa_file_data fd on fd.OID = em.fileDataOID
    where em.errorKey = 'EXPRESSION_ERROR'
		and expressionStatus = 'PENDING') as a)
and errorKey = 'EXPRESSION_ERROR';

#delete orphans fa_error_message for PRICE_ERROR
delete from fa_error_message where fileDataOID in(
select a.fileDataOID from (
select em.fileDataOID
	from fa_error_message em
		inner join fa_file_data fd on fd.OID = em.fileDataOID
    where em.errorKey = 'PRICE_ERROR'
		and priceStatus = 'PENDING') as a)
and errorKey = 'PRICE_ERROR';

#delete orphans fa_error_message for FACT_ERROR
delete from fa_error_message where fileDataOID in(
select a.fileDataOID from (
select em.fileDataOID
	from fa_error_message em
		inner join fa_file_data fd on fd.OID = em.fileDataOID
    where em.errorKey = 'FACT_ERROR'
		and factStatus = 'PENDING') as a)
and errorKey = 'FACT_ERROR';

#delete orphans fa_error_message for FILE_ERROR
delete from fa_error_message where fileDataOID in(
select a.fileDataOID from (
select em.fileDataOID
	from fa_error_message em
		inner join fa_file_data fd on fd.OID = em.fileDataOID
    where em.errorKey = 'FILE_ERROR'
		and fileStatus = 'PENDING') as a)
and errorKey = 'FILE_ERROR';

#delete orphans fa_period
delete from fa_period where oid in(
select c.oid from (
select p.oid
    from fa_period p
        left join fa_fact_value fv on fv.periodOID = p.OID
        left join fa_entity_fact_value efv on efv.periodOID = p.OID
        left join fa_custom_fact_value cfv on cfv.periodOID = p.OID
        left join fa_price pri on pri.periodOID = p.OID
where fv.OID is null
    and efv.OID is null
    and cfv.oid is null
    and pri.oid is null)as c);

#delete errorMessage empty
delete from fa_error_message where errorMessage = '';

######################################### quries

#search period duplicates
SELECT p1.OID
FROM fa_period p1
	join fa_period p2 on p1.oid != p2.oid and p1.endDate = p2.endDate
WHERE p1.endDate is not null AND p1.startDate IS NULL;

