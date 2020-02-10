update fa_file_data set copyStatus = 'PENDING' where priceStatus != 'OK';
update fa_file_data set calculateStatus = 'PENDING' where copyStatus != 'OK';
update fa_file_data set expressionStatus = 'PENDING' where (calculateStatus != 'OK' and calculateStatus != 'NO_DATA');


#delete copy, calculate and expression custom values
delete from fa_custom_fact_value where oid in(
select a.oid from(
select CFV.OID 
	from fa_custom_fact_value cfv
    inner join fa_file_data fd on cfv.fileDataOID = fd.OID
    where fd.copyStatus = 'PENDING') a);
    
#delete prices    
delete from fa_price where oid in(
select t.oid from (
select p.oid
	from fa_price p
    join fa_file_data fd on fd.oid = p.fileDataOID
    where fd.priceStatus = 'PENDING') as t);
                