with

orders_joined_addresses as (

    select * from {{ ref('orders_joined_addresses') }}

)

, final as (

    select
        state
        , count(order_guid) as number_of_orders

    from orders_joined_addresses
    group by state
    order by 2 desc
    limit 1

)

select * from final