use DBTEST

go

create table TRANSACTIONS_RAW
(
        STOCK_NAME  varchar (32)     not null
      , UT          int              not null
      , APRICE      numeric (22, 12) not null
      , AVOLUME     numeric (22, 12) not null
      , ID          bigint identity  not null

);


--******************************************************************************

use DBTEST

go

create or alter function TRUNC_UT (@p_UT bigint, @p_StripeTypeId int) returns bigint as
begin
    return
    case @p_StripeTypeId
    when 1  then @p_UT
    when 2  then @p_UT / 10 * 10
    when 3  then @p_UT / 60 * 60
    when 4  then @p_UT / 600 * 600
    when 5  then @p_UT / 3600 * 3600
    when 6  then @p_UT / 14400 * 14400
    when 7  then @p_UT / 86400 * 86400
    when 8  then datediff (second, cast ('1970-01-01 00:00:00' as datetime), dateadd(m,  datediff (m,  0, dateadd (second, @p_UT, cast ('1970-01-01 00:00:00' as datetime))), 0))
    when 9  then datediff (second, cast ('1970-01-01 00:00:00' as datetime), dateadd(yy, datediff (yy, 0, dateadd (second, @p_UT, cast ('1970-01-01 00:00:00' as datetime))), 0))
    when 10 then 0
    when 11 then 0
    end;
end;

go

create or alter function UT2DATESTR (@p_UT bigint) returns datetime as
begin
    return dateadd(s, @p_UT, cast ('1970-01-01 00:00:00' as datetime));
end;

go


--******************************************************************************

use DBTEST

go

create or alter view dbo.THINNING_HABR_SIMP_V as
with
  T1 (STRIPE_ID)
     as (select 1
         union all
         select STRIPE_ID + 1 from T1 where STRIPE_ID < 10)
, T2 as (select STRIPE_ID
              , STOCK_NAME
              , dbo.TRUNC_UT (UT, STRIPE_ID)             as UT
              , min (1000000 * cast (UT as bigint) + ID) as AOPEN_UT
              , max (APRICE)                             as AHIGH
              , min (APRICE)                             as ALOW
              , max (1000000 * cast (UT as bigint) + ID) as ACLOSE_UT
              , sum (AVOLUME)                            as AVOLUME
              , sum (APRICE * AVOLUME)                   as AAMOUNT
              , count (*)                                as ACOUNT
         from TRANSACTIONS_RAW, T1
         group by STRIPE_ID, STOCK_NAME, dbo.TRUNC_UT (UT, STRIPE_ID))
select t.STRIPE_ID, t.STOCK_NAME, t.UT, t_op.APRICE as AOPEN, t.AHIGH
     , t.ALOW, t_cl.APRICE as ACLOSE, t.AVOLUME, t.AAMOUNT, t.ACOUNT
from T2 t
join TRANSACTIONS_RAW t_op on (t.STOCK_NAME = t_op.STOCK_NAME and t.AOPEN_UT  / 1000000 = t_op.UT and t.AOPEN_UT  % 1000000 = t_op.ID)
join TRANSACTIONS_RAW t_cl on (t.STOCK_NAME = t_cl.STOCK_NAME and t.ACLOSE_UT / 1000000 = t_cl.UT and t.ACLOSE_UT % 1000000 = t_cl.ID);



--******************************************************************************

go

create table dbo.QUOTES_CALC
(
      STRIPE_ID   int not null
    , STOCK_NAME  varchar(32) not null
    , UT          bigint not null
    , AOPEN       numeric (22, 12) not null
    , AHIGH       numeric (22, 12) not null
    , ALOW        numeric (22, 12) not null
    , ACLOSE      numeric (22, 12) not null
    , AVOLUME     numeric (38, 12) not null
    , AAMOUNT     numeric (38, 12) not null
    , ACOUNT      int not null
);

go

create or alter procedure dbo.THINNING_HABR_CALC as
begin
    set nocount on;

    truncate table QUOTES_CALC;

    declare @StripeId int;

    with
      T1 as (select STOCK_NAME
                  , UT
                  , min (ID)                   as AOPEN_ID
                  , max (APRICE)               as AHIGH
                  , min (APRICE)               as ALOW
                  , max (ID)                   as ACLOSE_ID
                  , sum (AVOLUME)              as AVOLUME
                  , sum (APRICE * AVOLUME)     as AAMOUNT
                  , count (*)                  as ACOUNT
             from TRANSACTIONS_RAW
             group by STOCK_NAME, UT)
    insert into QUOTES_CALC (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
    select 1, t.STOCK_NAME, t.UT, t_op.APRICE, t.AHIGH, t.ALOW, t_cl.APRICE, t.AVOLUME, t.AAMOUNT, t.ACOUNT
    from T1 t
    join TRANSACTIONS_RAW t_op on (t.STOCK_NAME = t_op.STOCK_NAME and t.UT = t_op.UT and t.AOPEN_ID  = t_op.ID)
    join TRANSACTIONS_RAW t_cl on (t.STOCK_NAME = t_cl.STOCK_NAME and t.UT = t_cl.UT and t.ACLOSE_ID = t_cl.ID);

    set @StripeId = 1;

    while (@StripeId <= 9)
    begin

        with
          T1 as (select STOCK_NAME
                      , dbo.TRUNC_UT (UT, @StripeId + 1)    as UT
                      , min (UT)                            as AOPEN_UT
                      , max (AHIGH)                         as AHIGH
                      , min (ALOW)                          as ALOW
                      , max (UT)                            as ACLOSE_UT
                      , sum (AVOLUME)                       as AVOLUME
                      , sum (AAMOUNT)                       as AAMOUNT
                      , sum (ACOUNT)                        as ACOUNT
                 from QUOTES_CALC
                 where STRIPE_ID = @StripeId
                 group by STOCK_NAME, dbo.TRUNC_UT (UT, @StripeId + 1))
        insert into QUOTES_CALC (STRIPE_ID, STOCK_NAME, UT, AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, AAMOUNT, ACOUNT)
        select @StripeId + 1, t.STOCK_NAME, t.UT, t_op.AOPEN, t.AHIGH, t.ALOW, t_cl.ACLOSE, t.AVOLUME, t.AAMOUNT, t.ACOUNT
        from T1 t
        join QUOTES_CALC t_op on (t.STOCK_NAME = t_op.STOCK_NAME and t.AOPEN_UT  = t_op.UT)
        join QUOTES_CALC t_cl on (t.STOCK_NAME = t_cl.STOCK_NAME and t.ACLOSE_UT = t_cl.UT)
        where t_op.STRIPE_ID = @StripeId and t_cl.STRIPE_ID = @StripeId;

        set @StripeId = @StripeId + 1;

    end;

end;

go

create or alter view dbo.THINNING_HABR_CALC_V as
select *
from dbo.QUOTES_CALC;

go



--******************************************************************************

create or alter function dbo.THINNING_HABR_PPTF ()
returns @rettab table (
      STRIPE_ID  bigint           not null
    , STOCK_NAME varchar(32)      not null
    , UT         bigint           not null
    , AOPEN      numeric (22, 12) not null
    , AHIGH      numeric (22, 12) not null
    , ALOW       numeric (22, 12) not null
    , ACLOSE     numeric (22, 12) not null
    , AVOLUME    numeric (38, 12) not null
    , AAMOUNT    numeric (38, 12) not null
    , ACOUNT     bigint           not null)
as
begin

    declare @i tinyint;
    declare @tut int;

    declare @trans_STOCK_NAME varchar(32);
    declare @trans_UT int;
    declare @trans_ID int;
    declare @trans_APRICE numeric (22,12);
    declare @trans_AVOLUME numeric (22,12);

    declare @trans_prev_STOCK_NAME varchar(32);
    declare @trans_prev_UT int;
    declare @trans_prev_ID int;
    declare @trans_prev_APRICE numeric (22,12);
    declare @trans_prev_AVOLUME numeric (22,12);

    declare @QuoteTail table (
          STRIPE_ID  bigint           not null primary key clustered
        , STOCK_NAME varchar(32)      not null
        , UT         bigint           not null
        , AOPEN      numeric (22, 12) not null
        , AHIGH      numeric (22, 12)
        , ALOW       numeric (22, 12)
        , ACLOSE     numeric (22, 12)
        , AVOLUME    numeric (38, 12) not null
        , AAMOUNT    numeric (38, 12) not null
        , ACOUNT     bigint           not null);

    declare c cursor fast_forward for
    select STOCK_NAME, UT, ID, APRICE, AVOLUME
    from TRANSACTIONS_RAW
    order by STOCK_NAME, UT, ID; -- THIS ORDERING (STOCK_NAME, UT, ID) IS MANDATORY

    open c;

    fetch next from c into @trans_STOCK_NAME, @trans_UT, @trans_ID, @trans_APRICE, @trans_AVOLUME;

    while  @@fetch_status = 0
    begin

        if @trans_STOCK_NAME <> @trans_prev_STOCK_NAME or @trans_prev_STOCK_NAME is null
        begin
            insert into @rettab select * from @QuoteTail;
            delete @QuoteTail;
        end;

        set @i = 10;
        while @i >= 1
        begin
            set @tut = dbo.TRUNC_UT (@trans_UT, @i);

            if @tut <> (select UT from @QuoteTail where STRIPE_ID = @i)
            begin
                insert into @rettab select * from @QuoteTail where STRIPE_ID <= @i;
                delete @QuoteTail where STRIPE_ID <= @i;
            end;

            if (select count (*) from @QuoteTail where STRIPE_ID = @i) = 0
            begin
                insert into @QuoteTail (STRIPE_ID, STOCK_NAME, UT, AOPEN, AVOLUME, AAMOUNT, ACOUNT)
                values (@i, @trans_STOCK_NAME, @tut, @trans_APRICE, 0, 0, 0);
            end;

            update @QuoteTail
            set AHIGH = case when AHIGH < @trans_APRICE or AHIGH is null then @trans_APRICE else AHIGH end
              , ALOW = case when ALOW > @trans_APRICE or ALOW is null then @trans_APRICE else ALOW end
              , ACLOSE = @trans_APRICE, AVOLUME = AVOLUME + @trans_AVOLUME
              , AAMOUNT = AAMOUNT + @trans_APRICE * @trans_AVOLUME
              , ACOUNT = ACOUNT + 1
            where STRIPE_ID = @i;

            set @i = @i - 1;

        end;

        set @trans_prev_STOCK_NAME = @trans_STOCK_NAME;
        set @trans_prev_UT = @trans_UT;
        set @trans_prev_ID = @trans_ID;
        set @trans_prev_APRICE = @trans_APRICE;
        set @trans_prev_AVOLUME = @trans_AVOLUME;

        fetch next from c into @trans_STOCK_NAME, @trans_UT, @trans_ID, @trans_APRICE, @trans_AVOLUME;

    end;

    close c;
    deallocate c;

    insert into @rettab select * from @QuoteTail;

    return;

end;

go

create or alter view dbo.THINNING_HABR_PPTF_V as
select *
from dbo.THINNING_HABR_PPTF ();




