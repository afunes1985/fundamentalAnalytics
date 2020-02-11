#udate listed company with shares = 0
update fa_company set listed = 0, notListedDescription = 'sum EntityCommonStockSharesOutstanding = 0' 
where oid in (     
select a.oid from (SELECT c.oid
	FROM fa_entity_fact_value efv
		join fa_entity_fact ef on ef.oid = efv.entityFactOID
		join fa_file_data fd on fd.oid = efv.fileDataOID
        join fa_company	c on c.oid = fd.companyOID
        join fa_concept	co on co.oid = ef.conceptOID
    group by c.oid
	having sum(efv.value) = 0) as a);

update fa_file_data set copyStatus = 'PENDING' where priceStatus != 'OK';
update fa_file_data set calculateStatus = 'PENDING' where copyStatus != 'OK';
update fa_file_data set expressionStatus = 'PENDING' where (calculateStatus != 'OK' and calculateStatus != 'NO_DATA');

#update fd where listed = 0
update fa_file_data set copyStatus = 'PENDING', calculateStatus = 'PENDING', expressionStatus = 'PENDING', status = 'PENDING', priceStatus = 'PENDING'
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
    
#delete prices  where priceStatus = 'PENDING'  
delete from fa_price where oid in(
select t.oid from (
select p.oid
	from fa_price p
    join fa_file_data fd on fd.oid = p.fileDataOID
    where fd.priceStatus = 'PENDING') as t);

#delete fa_fact_report_relation where status = 'PENDING'
delete from fa_fact_report_relation where factOID in(    
select f.OID 
	from fa_file_data fd
		join fa_fact f on f.fileDataOID = fd.OID
	where status = 'PENDING');


#delete fa_fact where status = 'PENDING'
delete from fa_fact where OID in(    
	select a.OID from (
	select f.OID 
		from fa_file_data fd
			join fa_fact f on f.fileDataOID = fd.OID
		where status = 'PENDING') as a);

#delete fa_fact_value orphans
delete from fa_fact_value where oid in(
select a.oid from(
select fv.OID
from fa_fact_value fv
left join fa_fact f on fv.oid = f.factValueOID
where  f.oid is null)as a);


              


