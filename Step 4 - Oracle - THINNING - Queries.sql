-- Queries for duration measuring
-- Run one at a time

exec THINNING_HABR_CALC_T

select count (*) as CNT
     , sum (STRIPE_ID) as S_STRIPE_ID, sum (UT) as S_UT
     , sum (AOPEN) as S_AOPEN, sum (AHIGH) as S_AHIGH, sum (ALOW) as S_ALOW
     , sum (ACLOSE) as S_ACLOSE, sum (AVOLUME) as S_AVOLUME
     , sum (AAMOUNT) as S_AAMOUNT, sum (ACOUNT) as S_ACOUNT
from THINNING_HABR_SIMP_V

select count (*) as CNT
     , sum (STRIPE_ID) as S_STRIPE_ID, sum (UT) as S_UT
     , sum (AOPEN) as S_AOPEN, sum (AHIGH) as S_AHIGH, sum (ALOW) as S_ALOW
     , sum (ACLOSE) as S_ACLOSE, sum (AVOLUME) as S_AVOLUME
     , sum (AAMOUNT) as S_AAMOUNT, sum (ACOUNT) as S_ACOUNT
from THINNING_HABR_CHIN_V

select count (*) as CNT
     , sum (STRIPE_ID) as S_STRIPE_ID, sum (UT) as S_UT
     , sum (AOPEN) as S_AOPEN, sum (AHIGH) as S_AHIGH, sum (ALOW) as S_ALOW
     , sum (ACLOSE) as S_ACLOSE, sum (AVOLUME) as S_AVOLUME
     , sum (AAMOUNT) as S_AAMOUNT, sum (ACOUNT) as S_ACOUNT
from THINNING_HABR_PPTF_V


-- Query for comparing results

with
  T1 as (select 'SIMP' as ALG_NAME, a.* from THINNING_HABR_SIMP_V a
         union all
         select 'CALC', a.* from THINNING_HABR_CALC_V a
         union all
         select 'CHIN', a.* from THINNING_HABR_CHIN_V a
         union all
         select 'PPTF', a.* from THINNING_HABR_PPTF_V a)
select ALG_NAME
     , count (*) as CNT
     , sum (STRIPE_ID) as S_STRIPE_ID, sum (UT) as S_UT
     , sum (AOPEN) as S_AOPEN, sum (AHIGH) as S_AHIGH, sum (ALOW) as S_ALOW
     , sum (ACLOSE) as S_ACLOSE, sum (AVOLUME) as S_AVOLUME
     , sum (AAMOUNT) as S_AAMOUNT, sum (ACOUNT) as S_ACOUNT
from T1
group by ALG_NAME;

