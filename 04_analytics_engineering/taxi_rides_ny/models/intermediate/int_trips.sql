with unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'u.vendorid',
            'u.pickup_datetime',
            'u.dropoff_datetime',
            'u.pickup_locationid',
            'u.dropoff_locationid',
            'u.service_type'
        ]) }} as trip_id,

        u.*,
        coalesce(u.payment_type,0) as payment_type,
        coalesce(pt.description,'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type,0) = pt.payment_type
),

deduplicated as (

    select *
    from cleaned_and_enriched
    qualify row_number() over(
        partition by vendorid, pickup_datetime, pickup_locationid, service_type
        order by dropoff_datetime
    ) = 1

)

select * from deduplicated