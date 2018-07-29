-- Run under THINNING user

create table TRANSACTIONS_RAW (
      ID            number not null
    , STOCK_NAME    varchar2 (32)
    , UT            number not null
    , APRICE        number not null
    , AVOLUME       number not null)
pctfree 0 parallel 4 nologging;


--******************************************************************************

create or replace function TRUNC_UT (p_UT number, p_StripeTypeId number)
return number deterministic is
begin
    return
    case p_StripeTypeId
    when 1  then trunc (p_UT / 1) * 1
    when 2  then trunc (p_UT / 10) * 10
    when 3  then trunc (p_UT / 60) * 60
    when 4  then trunc (p_UT / 600) * 600
    when 5  then trunc (p_UT / 3600) * 3600
    when 6  then trunc (p_UT / ( 4 * 3600)) * ( 4 * 3600)
    when 7  then trunc (p_UT / (24 * 3600)) * (24 * 3600)
    when 8  then trunc ((trunc (date '1970-01-01' + p_UT / 86400, 'Month') - date '1970-01-01') * 86400)
    when 9  then trunc ((trunc (date '1970-01-01' + p_UT / 86400, 'year')  - date '1970-01-01') * 86400)
    when 10 then 0
    when 11 then 0
    end;
end;


create or replace function UT2DATESTR (p_UT number) return varchar2 deterministic is
begin
    return to_char (date '1970-01-01' + p_UT / 86400, 'YYYY.MM.DD HH24:MI:SS');
end;


--******************************************************************************

create or replace view THINNING_HABR_SIMP_V as
select STRIPE_ID
     , STOCK_NAME
     , TRUNC_UT (UT, STRIPE_ID)                             as UT
     , avg (APRICE) keep (dense_rank first order by UT, ID) as AOPEN
     , max (APRICE)                                         as AHIGH
     , min (APRICE)                                         as ALOW
     , avg (APRICE) keep (dense_rank last  order by UT, ID) as ACLOSE
     , sum (AVOLUME)                                        as AVOLUME
     , sum (APRICE * AVOLUME)                               as AAMOUNT
     , count (*)                                            as ACOUNT
from TRANSACTIONS_RAW
  , (select rownum as STRIPE_ID from dual connect by level <= 10)
group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID);


--******************************************************************************

create table QUOTES_CALC (
      STRIPE_ID     number not null
    , STOCK_NAME    varchar2 (128) not null
    , UT            number not null
    , AOPEN         number not null
    , AHIGH         number not null
    , ALOW          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , AAMOUNT       number not null
    , ACOUNT        number not null
)
/*partition by list (STRIPE_ID) (
      partition P01 values (1)
    , partition P02 values (2)
    , partition P03 values (3)
    , partition P04 values (4)
    , partition P05 values (5)
    , partition P06 values (6)
    , partition P07 values (7)
    , partition P08 values (8)
    , partition P09 values (9)
    , partition P10 values (10)
)*/
parallel 4 pctfree 0 nologging;

create or replace procedure THINNING_HABR_CALC_T is
begin

    rollback;

    execute immediate 'truncate table QUOTES_CALC';

    insert --+ append
    into QUOTES_CALC
    select 1 as STRIPE_ID
         , STOCK_NAME
         , UT
         , avg (APRICE) keep (dense_rank first order by ID)
         , max (APRICE)
         , min (APRICE)
         , avg (APRICE) keep (dense_rank last  order by ID)
         , sum (AVOLUME)
         , sum (APRICE * AVOLUME)
         , count (*)
    from TRANSACTIONS_RAW a
    group by STOCK_NAME, UT;

    commit;

    for i in 1..9
    loop

        insert --+ append
        into QUOTES_CALC
        select --+ parallel(4)
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, i + 1)
             , avg (AOPEN)   keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE)  keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from QUOTES_CALC a
        where STRIPE_ID = i
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, i + 1);

        commit;

    end loop;

end;
/


create view THINNING_HABR_CALC_V as
select * from QUOTES_CALC;


--******************************************************************************

create or replace view THINNING_HABR_CHIN_V as
with
  T01 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select 1
            , STOCK_NAME
            , UT
            , avg (APRICE) keep (dense_rank first order by ID)
            , max (APRICE)
            , min (APRICE)
            , avg (APRICE) keep (dense_rank last  order by ID)
            , sum (AVOLUME)
            , sum (APRICE * AVOLUME)
            , count (*)
       from TRANSACTIONS_RAW
       group by STOCK_NAME, UT)
, T02 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T01
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T03 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T02
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T04 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T03
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T05 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T04
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T06 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T05
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T07 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T06
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T08 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T07
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T09 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T08
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
, T10 (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
   as (select
               STRIPE_ID + 1
             , STOCK_NAME
             , TRUNC_UT (UT, STRIPE_ID + 1)
             , avg (AOPEN)  keep (dense_rank first order by UT)
             , max (AHIGH)
             , min (ALOW)
             , avg (ACLOSE) keep (dense_rank last  order by UT)
             , sum (AVOLUME)
             , sum (AAMOUNT)
             , sum (ACOUNT)
        from T09
        group by STRIPE_ID, STOCK_NAME, TRUNC_UT (UT, STRIPE_ID + 1))
select * from T01 union all
select * from T02 union all
select * from T03 union all
select * from T04 union all
select * from T05 union all
select * from T06 union all
select * from T07 union all
select * from T08 union all
select * from T09 union all
select * from T10;


--******************************************************************************

create or replace package THINNING_PPTF_P is

    type TRANSACTION_RECORD_T is
    record (STOCK_NAME varchar2(128), UT number, SEQ_NUM number, APRICE number, AVOLUME number);

    type CUR_RECORD_T is ref cursor return TRANSACTION_RECORD_T;

    type QUOTE_T
    is record (STRIPE_ID number, STOCK_NAME varchar2(128), UT number
             , AOPEN number, AHIGH number, ALOW number, ACLOSE number, AVOLUME number
             , AAMOUNT number, ACOUNT number);

    type QUOTE_LIST_T is table of QUOTE_T;

    function F (p_cursor CUR_RECORD_T) return QUOTE_LIST_T
    pipelined order p_cursor by (STOCK_NAME, UT, SEQ_NUM)
    parallel_enable (partition p_cursor by hash (STOCK_NAME));

end;
/

create or replace package body THINNING_PPTF_P is

function F (p_cursor CUR_RECORD_T) return QUOTE_LIST_T
pipelined order p_cursor by (STOCK_NAME, UT, SEQ_NUM)
parallel_enable (partition p_cursor by hash (STOCK_NAME))
is
    QuoteTail QUOTE_LIST_T := QUOTE_LIST_T() ;
    rec TRANSACTION_RECORD_T;
    rec_prev TRANSACTION_RECORD_T;
    type ut_T is table of number index by pls_integer;
    ut number;
begin

    QuoteTail.extend(10);

    loop
        fetch p_cursor into rec;
        exit when p_cursor%notfound;

        if rec_prev.STOCK_NAME = rec.STOCK_NAME
        then
            if    (rec.STOCK_NAME = rec_prev.STOCK_NAME and rec.UT < rec_prev.UT)
               or (rec.STOCK_NAME = rec_prev.STOCK_NAME and rec.UT = rec_prev.UT and rec.SEQ_NUM < rec_prev.SEQ_NUM)
            then raise_application_error (-20010, 'Rowset must be ordered, ('||rec_prev.STOCK_NAME||','||rec_prev.UT||','||rec_prev.SEQ_NUM||') > ('||rec.STOCK_NAME||','||rec.UT||','||rec.SEQ_NUM||')');
            end if;
        end if;


        if rec.STOCK_NAME <> rec_prev.STOCK_NAME or rec_prev.STOCK_NAME is null
        then
            for j in 1 .. 10
            loop
                if QuoteTail(j).UT is not null
                then
                    pipe row (QuoteTail(j));
                    QuoteTail(j) := null;
                end if;
            end loop;
        end if;

        for i in reverse 1..10
        loop
            ut := TRUNC_UT (rec.UT, i);

            if QuoteTail(i).UT <> ut
            then
                for j in 1..i
                loop
                    pipe row (QuoteTail(j));
                    QuoteTail(j) := null;
                end loop;
            end if;

            if QuoteTail(i).UT is null
            then
                 QuoteTail(i).STRIPE_ID := i;
                 QuoteTail(i).STOCK_NAME := rec.STOCK_NAME;
                 QuoteTail(i).UT := ut;
                 QuoteTail(i).AOPEN := rec.APRICE;
            end if;

            if rec.APRICE < QuoteTail(i).ALOW or QuoteTail(i).ALOW is null then QuoteTail(i).ALOW := rec.APRICE; end if;
            if rec.APRICE > QuoteTail(i).AHIGH or QuoteTail(i).AHIGH is null then QuoteTail(i).AHIGH := rec.APRICE; end if;
            QuoteTail(i).AVOLUME := nvl (QuoteTail(i).AVOLUME, 0) + rec.AVOLUME;
            QuoteTail(i).AAMOUNT := nvl (QuoteTail(i).AAMOUNT, 0) + rec.AVOLUME * rec.APRICE;
            QuoteTail(i).ACOUNT := nvl (QuoteTail(i).ACOUNT, 0) + 1;
            QuoteTail(i).ACLOSE := rec.APRICE;

        end loop;

        rec_prev := rec;
    end loop;

    for j in 1 .. 10
    loop
        if QuoteTail(j).UT is not null
        then
            pipe row (QuoteTail(j));
        end if;
    end loop;

exception
    when no_data_needed then null;
end;

end;
/

create or replace view THINNING_HABR_PPTF_V as
select * from table (THINNING_PPTF_P.F (cursor (select STOCK_NAME, UT, ID, APRICE, AVOLUME from TRANSACTIONS_RAW)));


