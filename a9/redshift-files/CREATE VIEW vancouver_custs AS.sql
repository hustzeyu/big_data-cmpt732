CREATE VIEW vancouver_custs AS
WITH 
  vprefixes (vp) AS 
    (SELECT DISTINCT pcprefix FROM greater_vancouver_prefixes)
SELECT customers.custid, ( CASE WHEN (substring(customers.postalcode,1, 3)=vprefixes.vp) THEN 1
                ELSE 0
                END      )AS in_vancouver
FROM customers left outer join vprefixes on (substring(customers.postalcode,1, 3)=vprefixes.vp);

--------------------------------------------------------------------------------------------------------------From_BC_non_Van



drop view dev.public.from_bc_non_van_custs;
select count(*) from From_BC_non_Van_custs;


CREATE VIEW From_BC_non_Van_custs AS
SELECT customers.custid, ( CASE WHEN ((vancouver_custs.in_vancouver = 0) and (customers.province = 'BC'))  THEN 1
                ELSE 0
                END      )AS From_BC_non_Van
FROM customers inner join vancouver_custs ON (customers.custid = vancouver_custs.custid) ;


---------------------------------------------------------------------------------------


SELECT From_BC_non_Van_custs.From_BC_non_Van as From_BC_non_Van, 
        vancouver_custs.in_vancouver as From_Van, 
        (COUNT(*)) as Count,  
        (AVG(purchases.amount)) as Average, 
        (median(purchases.amount)) as Median
FROM  From_BC_non_Van_custs join vancouver_custs on (From_BC_non_Van_custs.custid = vancouver_custs.custid)
join purchases on (From_BC_non_Van_custs.custid = purchases.custid)
group by From_BC_non_Van_custs.From_BC_non_Van, vancouver_custs.in_vancouver
order by Median;

------------------------------------------------------------------------------


WITH 
  sushi (amenid ) AS 
    (SELECT amenid FROM amenities where(tags.cuisine ilike '%sushi%' or tags.cuisine ilike '%udon%' or tags.cuisine ilike '%sashimi%' ))
SELECT (avg(purchases.amount)) as avg,
        vancouver_custs.in_vancouver as in_vancouver
FROM purchases left join vancouver_custs on (purchases.custid = vancouver_custs.custid)
    left join sushi on (purchases.amenid = sushi.amenid)
where (purchases.amenid = sushi.amenid)
group by in_vancouver
order by in_vancouver;


select pdate, (avg(amount)) as avg 
from purchases
where (date_part(mon,pdate)=08)
group by pdate
order by pdate
limit 5;