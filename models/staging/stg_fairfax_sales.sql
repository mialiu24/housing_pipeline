with source as (
    select * from {{ source('fairfax_raw', 'FAIRFAX_SALES') }}
),

renamed as (
    select
        objectid,
        parid                                           as parcel_id,
        to_timestamp(saledt / 1000)::date               as sale_date,
        saleprice                                       as sale_price,
        saletype                                        as sale_type,
        instrtyp                                        as instrument_type,
        booknbr                                         as book_number,
        pagenbr                                         as page_number
    from source
    where saledt is not null
      and saleprice > 0
)

select * from renamed
