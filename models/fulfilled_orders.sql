/*
    Model that filters Orders to pull only fulfilled
    orders: order status = F
*/

{{ config(
    materialized='view') }}

with all_orders as (

    select * from SLOBO.ORDERS

)

select *
from all_orders where O_ORDERSTATUS = 'F' and O_ORDERKEY is not null
