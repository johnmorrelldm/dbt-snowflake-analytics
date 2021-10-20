/*
    Model that takes fulfilled_orders model
    aggregates addtition info and joins
    with customer and nation data for a
    complete look at fulfilled orders
    for each customer.
*/

{{ config(
    materialized='view') }}

with customer_orders as (
    select
        O_CUSTKEY as customer_id,
        min(O_ORDERDATE) as first_order_date,
        max(O_ORDERDATE) as most_recent_order_date,
        min(O_TOTALPRICE) as smallest_order,
        max(O_TOTALPRICE) as largest_order,
        count(O_ORDERKEY) as number_of_orders
    from {{ ref('fulfilled_orders') }}
    group by 1
),

customers as(
select
    CUSTOMER.C_CUSTKEY as customer_id,
    CUSTOMER.C_NAME as customer_name,
    CUSTOMER.C_ADDRESS as customer_address,
    CUSTOMER.C_MKTSEGMENT as customer_segment,
    CUSTOMER.C_NATIONKEY AS nation_id
from SLOBO.CUSTOMER
),

nations as (

select
    NATION.N_NATIONKEY AS nation_id,
    NATION.N_NAME AS nation_name
from SLOBO.NATION
),

final as (
select
    customers.customer_id,
    customers.customer_name,
    customers.customer_address,
    nations.nation_name,
    customers.customer_segment,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    customer_orders.smallest_order,
    customer_orders.largest_order,
    coalesce(customer_orders.number_of_orders, 0) as number_of_orders
from customer_orders
left join customers using (customer_id)
left join nations using (nation_id)
)

select * from final
