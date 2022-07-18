with

fact_customer_subscriptions_monthly as (

    select
        accounting_month,
        salesforce_account_id,
        monthly_recurring_bookings_net_discounts_usd                             as monthly_recurring_bookings_usd,
        product_group,
        product_category,
        product,
        top_hierarchy_account_name,
        top_hierarchy_account_id,
        top_hierarchy_type
    from {{ ref('fact_customer_subscriptions') }}
    where (is_active_subscriber or is_active_add_on_subscriber)
        and accounting_month >= '2021-04-01' and accounting_month < date_trunc('MONTH',current_date())
        and platform = 'Booker'
        and try_to_numeric(core_product_instance_id) is not null
        and (product_category = 'Core Software'
             or (product_category = 'Add-On'
                 and product_group = 'Marketing Suite'))

),

booker_rev_consolidated as (

    select
        salesforce_account_id,
        accounting_month,
        top_hierarchy_account_name,
        top_hierarchy_account_id,
        top_hierarchy_type,
        sum(monthly_recurring_bookings_usd)                                       as booker_mrr
    from fact_customer_subscriptions_monthly
    {{ dbt_utils.group_by(n=5) }}

),

final as (

    select *
    from booker_rev_consolidated
    where accounting_month >='2021-04-01'


)

select * from final
