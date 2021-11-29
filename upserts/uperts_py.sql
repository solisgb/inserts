select *
from ins.captaciones c 
where c.x is null or c.y is null or c.geom is null
;

--alter table ipa.ipa2 
drop column codigosonda
;

--ALTER TABLE ipas.ipa2 drop CONSTRAINT ipa2_codigosonda_fkeys;


--create table tmp.ipasub
as
select * from ipas.ipa1 limit 5
;

select column_name, data_type
from information_schema.columns
where table_catalog='ipa' and table_schema ='ipas'
    and table_name = 'ipa1'
order by ordinal_position;


SELECT c.column_name, c.data_type
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and constraint_schema='ipas' and tc.table_name = 'ipa1';
;
