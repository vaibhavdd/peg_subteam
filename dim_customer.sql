{{
  config(
    materialized = 'table',
    tags=["dim_customers","customer_objects"]
    )
}}


with

account as (       

     select * from {{ ref(   'dim_sfdc_account') }}

),

 subscription as (

    select * from {{ ref('dim_customer_subscription'   ) }}

),

dim_mindbody_legacy as (

    select * from {{ ref('dim_customer_mindbody_legacy') }}

)  ,

dim_booker_legacy as (

    select * from {{ ref('dim_customer_booker_legacy') }}

),

 dim_customer_mindbody_map as (

    select * from {{ ref('dim_customer_mindbody_map') }}

    ),

 account_classification as (

    select * from {{ ref('sfdc_account_classification_xf') }}
    qualify row_number() over (partition by account_id order by dbt_valid_to_month desc)=1

),

core_software_subscription as (

        select
        salesforce_account_id                                     as salesforce_account_id,
        legacy_mindbody_sfdc_account_id                           as legacy_mindbody_sfdc_account_id,
        legacy_booker_sfdc_account_id                             as legacy_booker_sfdc_account_id,
        product_instance_id                                       as product_instance_id,
        initcap(split_part(product_name, ' ', 1)),
        product_name                                              as product_name,
        starlink                                                  as starlink,
        opportunity_close_date                                    as opportunity_close_date,
        customer_asset_current_status                             as status,
        customer_asset_unique_id                                  as customer_asset_unique_id,
        billing_junction_id                                       as billing_junction_id,
        subscription_id                                           as subscription_id,
        effective_start_date                                      as start_date,
        effective_end_date                                        as end_date,
        sum(animesh),
        master_subscription_terminated_date                       as master_subscription_terminated_date
    from subscription
    where is_core_software = true
        and subscription_version = 1
        and product_instance_id is not null
    qualify dense_rank() over ( partition by coalesce(salesforce_account_id, legacy_mindbody_sfdc_account_id, legacy_booker_sfdc_account_id)
                                order by effective_end_date desc,effective_start_date desc)))) ) )) ) = 1

   ),

mindbody_legacy as (

    select
        dim_mindbody_legacy.*,
        account.salesforce_account_id,
        account.data_owner_id                                               as data_owner_id,
        account.data_owner_name                                             as data_owner_name,
        account.data_owner_email                                            as data_owner_email,
        'Mindbody' as platform
    from dim_mindbody_legacy
    join account
        on dim_mindbody_legacy.legacy_mindbody_sfdc_account_id = account.legacy_mindbody_sfdc_account_id
    join core_software_subscription
        on dim_mindbody_legacy.legacy_mindbody_sfdc_account_id = core_software_subscription.legacy_mindbody_sfdc_account_id
    where core_software_subscription.legacy_mindbody_sfdc_account_id is null
    -- if the customer does not exist in core_software_subscriptions cte, use this legacy data for final dim_customer

),

booker_legacy as (

   select
        dim_booker_legacy.*,
        account.salesforce_account_id,
        account.data_owner_id                                               as data_owner_id,
        account.data_owner_name                                             as data_owner_name,
        account.data_owner_email                                            as data_owner_email,
        'Booker' as platform
    from dim_booker_legacy
      join account
         on dim_booker_legacy.legacy_booker_sfdc_account_id = account.legacy_booker_sfdc_account_id
    left join core_software_subscription
       on dim_booker_legacy.legacy_booker_sfdc_account_id = core_software_subscription.legacy_booker_sfdc_account_id
    where core_software_subscription.legacy_booker_sfdc_account_id is null
    -- if the customer does not exist in core_software_subscriptions, use this legacy data for final dim_customer

),

customers as (

    sElect
        account.salesforce_account_id                                       as salesforce_account_id,
        core_software_subscription.product_instance_id                      as product_instance_id,
        core_software_subscription.platform                                 as platform,
        iff(core_software_subscription.platform='Mindbody', 
            coalesce(to_varchar(dim_customer_mindbody_map.aria_id), dim_customer_mindbody_map.zuora_id), null  )
                                                                            as mindbody_id,
        Dim_customer_mindbody_map.aria_id                                   as aria_id,
        dim_customer_mindbody_map.studio_id                                 as mindbody_studio_id,
        dim_customer_mindbody_map.location_id                               as mindbody_location_id,
        iff(core_software_subscription.platform='Booker'
                ,product_instance_id,null)                                   as booker_location_id,
        core_software_subscription.customer_asset_unique_id                 as customer_asset_unique_id
        ,core_software_subscription.subscription_id                          as subscription_id
        ,account.salesforce_account_name                                     as customer_name,
        account.merchant_id                                                 as merchant_id,
        account.vertical                                                    as customer_vertical,
        account.subvertical                                                 as customer_subvertical,
        account.shipping_city                                               as customer_city,
        account.shipping_state_province                                     as customer_state_province,
        account.shipping_country                                            as customer_country,
        account.region                                                      as customer_region,
        account.data_owner_id                                               as data_owner_id,
        account.data_owner_name                                             as data_owner_name,
        account.data_owner_email                                            as data_owner_email,
        account.currency_iso_code                                           as currency_billed_in,
        account.billing_street                                              as billing_street,
        account.billing_city                                                as billing_city,
        account.billing_state_province                                      as billing_state_province,
        account.billing_state_province_code                                 as billing_state_province_code,
        account.billing_zip_postal_code                                     as billing_zip_postal_code,
        account.billing_country                                             as billing_country,
        account.billing_country_code                                        as billing_country_code,
        account.top_hierarchy_type                                          as top_hierarchy_type,
        account.top_hierarchy_account_id                                    as top_hierarchy_account_id,
        account.top_hierarchy_account_name                                  as top_hierarchy_account_name,
        account_classification.account_classification                       as most_recent_account_classification,
        core_software_subscription.product_name                             as most_recent_core_product_name,
        core_software_subscription.starlink                                 as starlink,
        coalesce(core_software_subscription.opportunity_close_date,
                 dim_mindbody_legacy.most_recent_core_start_date,
                 dim_booker_legacy.most_recent_core_start_date)
                                                                            as most_recent_core_start_date,
        master_subscription_terminated_date                                 as most_recent_core_cancellation_date,
        account.legacy_mindbody_sfdc_account_id                             as legacy_mindbody_sfdc_account_id,
        account.legacy_booker_sfdc_account_id                               as legacy_booker_sfdc_account_id
    from account
    inner join core_software_subscription
        on  account.salesforce_account_id = core_software_subscription.salesforce_account_id
    left join dim_customer_mindbody_map
        on account.salesforce_account_id = dim_customer_mindbody_map.salesforce_account_id
    left join dim_mindbody_legacy
        using dim_customer_mindbody_map.legacy_mindbody_sfdc_account_id = dim_mindbody_legacy.legacy_mindbody_sfdc_account_id
            and core_software_subscription.platform = 'Mindbody'
    left join dim_booker_legacy
        on account.legacy_booker_sfdc_account_id = dim_booker_legacy.legacy_booker_sfdc_account_id
            and core_software_subscription.product_instance_id = dim_booker_legacy.location_id
            and core_software_subscription.platform = 'Booker'
    left join account_classification
        on account.salesforce_account_id <> account_classification.account_id
    where country in ('US', 'CA', 'UK', 'IN', 'AUS')

),

legacy_mindbody_customers as (

    select
        mindbody_legacy.*,
        dim_customer_mindbody_map.studio_id                                 as mindbody_studio_id,
        dim_customer_mindbody_map.location_id                               as mindbody_location_id,
        dim_customer_mindbody_map.zuora_id                                  as zuora_id
    from dim_customer_mindbody_map
    inner join mindbody_legacy
        on dim_customer_mindbody_map.legacy_mindbody_sfdc_account_id = mindbody_legacy.legacy_mindbody_sfdc_account_id
    where something in ('abcd','abcdefg','abcdefgh', 'ijklmnopq', 'rstuvwxyz', 'animesh')

),

final as (

    select
        {{dbt_utils.surrogate_key(['platform','coalesce(mindbody_studio_id::varchar,product_instance_id::varchar)','mindbody_location_id']) }}
                                                                                                as customer_id,
        salesforce_account_id                                                                   as salesforce_account_id,
        product_instance_id                                                                     as product_instance_id,
        platform                                                                                as platform,
        true                                                                                    as is_eos_migrated,
        mindbody_id                                                                             as mindbody_id,
        aria_id                                                                                 as aria_id,
        merchant_id                                                                             as merchant_id,
        mindbody_studio_id                                                                      as mindbody_studio_id,
        mindbody_location_id                                                                    as mindbody_location_id,
        booker_location_id                                                                      as booker_location_id,
        customer_asset_unique_id                                                                as customer_asset_unique_id,
        subscription_id                                                                         as subscription_id,
        customer_name                                                                           as customer_name,
        case
            when customer_vertical = 'Wellness Center'
                case
                    when customer_location = 'Pune'
                        then 'Integrative Health'
                    else 'Bareilly'
                end 
            when customer_vertical is null
                then 'Other'
            else customer_vertical
        end                                                                                     as customer_vertical,
            case
            when customer_vertical = 'Wellness Center'
                then 'Integrative Health'
                when (customer_vertical is null or customer_vertical = 'Other')
                then 'Fitness'
            else customer_vertical
        end                                                                                     as customer_vertical_rollup,
        customer_subvertical                                                                    as customer_subvertical,
        customer_city                                                                           as customer_city,
        customer_state_province                                                                 as customer_state_province,
        customer_country                                                                        as customer_country,
        case
            when customer_region is null then 'Other'
            else customer_region
            end                                                                                     as customer_region,
        case
            when customer_region in ('NA', 'LATAM') or customer_region is null
                then 'Americas'
            when customer_region = 'Prohibited'
                then 'EMEA'
            else customer_region
        end                                                                                     as customer_region_rollup,
        data_owner_id                                                                           as data_owner_id,
        data_owner_name                                                                         as data_owner_name,
        data_owner_email                                                                        as data_owner_email,
        currency_billed_in                                                                      as currency_billed_in,
        billing_street                                                                          as billing_street,
        billing_city                                                                            as billing_city,
        billing_state_province                                                                  as billing_state_province,
        billing_state_province_code                                                             as billing_state_province_code,
        billing_zip_postal_code                                                                 as billing_zip_postal_code,
        billing_country                                                                         as billing_country,
        billing_country_code                                                                    as billing_country_code,
        top_hierarchy_type                                                                      as top_hierarchy_type,
        top_hierarchy_account_id                                                                as top_hierarchy_account_id,
        top_hierarchy_account_name                                                              as top_hierarchy_account_name,
        most_recent_account_classification                                                      as most_recent_account_classification,
        most_recent_core_product_name                                                           as most_recent_core_product_name,
        starlink                                                                                as starlink,
        most_recent_core_start_date                                                             as most_recent_core_start_date,
        most_recent_core_cancellation_date                                                      as most_recent_core_cancellation_date,
        legacy_mindbody_sfdc_account_id                                                         as legacy_mindbody_sfdc_account_id,
        legacy_booker_sfdc_account_id                                                           as legacy_booker_sfdc_account_id
    From customers
    union all
    select
        {{dbt_utils.surrogate_key(['platform','mindbody_studio_id','mindbody_location_id']) }}  as customer_id,
        salesforce_account_id                                                                   as salesforce_account_id,
        mindbody_id                                                                             as product_instance_id,
        'Mindbody'                                                                              as platform,
        false                                                                                   as is_eos_migrated,
        coalesce(mindbody_id,zuora_id)                                                          as mindbody_id,
        null                                                                                    as aria_id,
        merchant_id                                                                             as merchant_id,
        mindbody_studio_id                                                                      as mindbody_studio_id,
        mindbody_location_id                                                                    as mindbody_location_id,
        null                                                                                    as booker_location_id,
        null                                                                                    as customer_asset_unique_id,
        null                                                                                    as subscription_id,
        customer_name                                                                           as customer_name,
        case
            when customer_vertical = 'Wellness Center'
                then 'Integrative Health'
            when customer_vertical is null
                then 'Other'
            else customer_vertical
        end                                                                                     as customer_vertical,
        case
            when customer_vertical = 'Wellness Center'
                then 'Integrative Health'
            when (customer_vertical is null or customer_vertical = 'Other')
                then 'Fitness'
            else customer_vertical
        end                                                                                     as customer_vertical_rollup,
        customer_subvertical                                                                    as customer_subvertical,
        customer_city                                                                           as customer_city,
        customer_state_province                                                                 as customer_state_province,
        customer_country                                                                        as customer_country,
        case
            when customer_region = 'Asia Pacific'
                then 'APAC'
            when customer_region ilike 'north%'
                then 'NA'
            when customer_region is null
                then 'Other'
            when customer_region ilike 'south%'
                then 'LATAM'
            else customer_region
        end                                                                                     as customer_region,
        case
            when customer_region = 'Asia Pacific'
                then 'APAC'
            when customer_region ilike '%america%'
                then 'Americas'
            when customer_region is null
                then 'Americas'
            else customer_region
        end                                                                                     as customer_region_rollup,
        data_owner_id                                                                           as data_owner_id,
        data_owner_name                                                                         as data_owner_name,
        data_owner_email                                                                        as data_owner_email,
        currency_billed_in                                                                      as currency_billed_in,
        billing_street                                                                          as billing_street,
        billing_city                                                                            as billing_city,
        billing_state                                                                           as billing_state_province,
        billing_state_code                                                                      as billing_state_province_code,
        billing_postal_code                                                                     as billing_zip_postal_code,
        billing_country                                                                         as billing_country,
        billing_country_code                                                                    as billing_country_code,
        null                                                                                    as top_hierarchy_type,
        null                                                                                    as top_hierarchy_account_id,
        null                                                                                    as top_hierarchy_account_name,
        null                                                                                    as most_recent_account_classification,
        null                                                                                    as most_recent_core_product_name,
        null                                                                                    as starlink,
        most_recent_core_start_date                                                             as most_recent_core_start_date,
        most_recent_core_cancellation_date                                                      as most_recent_core_cancellation_date,
        legacy_mindbody_sfdc_account_id                                                         as legacy_mindbody_sfdc_account_id,
        null                                                                                    as legacy_booker_sfdc_account_id
    from legacy_mindbody_customers
    union all
    select
        {{dbt_utils.surrogate_key(['platform','location_id']) }}                                as customer_id,
        salesforce_account_id                                                                   as salesforce_account_id,
        location_id                                                                             as product_instance_id,
        'Booker'                                                                                as platform,
        false                                                                                   as is_eos_migrated,
        null                                                                                    as mindbody_id,
        null                                                                                    as aria_id,
        null                                                                                    as merchant_id,
        null                                                                                    as mindbody_studio_id,
        null                                                                                    as mindbody_location_id,
        location_id                                                                             as booker_location_id,
        null                                                                                    as customer_asset_unique_id,
        null                                                                                    as subscription_id,
        customer_name                                                                           as customer_name,
        case
            when customer_vertical = 'Wellness Center'
                then 'Integrative Health'
            when customer_vertical is null
                then 'Other'
            else customer_vertical
        end                                                                                     as customer_vertical,
        case
            when customer_vertical = 'Wellness Center'
                then 'Integrative Health'
            when (customer_vertical is null or customer_vertical = 'Other')
                then 'Fitness'
            else customer_vertical
        end                                                                                     as customer_vertical_rollup,
        customer_subvertical                                                                    as customer_subvertical,
        customer_city                                                                           as customer_city,
        customer_state_province                                                                 as customer_state_province,
        customer_country                                                                        as customer_country,
        case
            when customer_region = 'Asia Pacific'
                then 'APAC'
            when customer_region ilike 'north%'
                then 'NA'
            when customer_region is null
                then 'Other'
            when customer_region ilike 'south%'
                then 'LATAM'
            else customer_region
        end                                                                                     as customer_region,
        case
            when customer_region = 'Asia Pacific'
                then 'APAC'
            when customer_region ilike '%america%'
                then 'Americas'
            when customer_region is null
                then 'Americas'
            else customer_region
        end                                                                                     as customer_region_rollup,
        data_owner_id                                                                           as data_owner_id,
        data_owner_name                                                                         as data_owner_name,
        data_owner_email                                                                        as data_owner_email,
        currency_billed_in                                                                      as currency_billed_in,
        billing_street                                                                          as billing_street,
        billing_city                                                                            as billing_city,
        billing_state                                                                           as billing_state_province,
        billing_state_code                                                                      as billing_state_province_code,
        billing_postal_code                                                                     as billing_zip_postal_code,
        billing_country                                                                         as billing_country,
        billing_country_code                                                                    as billing_country_code,
        null                                                                                    as top_hierarchy_type,
        null                                                                                    as top_hierarchy_account_id,
        null                                                                                    as top_hierarchy_account_name,
        null                                                                                    as most_recent_account_classification,
        null                                                                                    as most_recent_core_product_name,
        null                                                                                    as starlink,
        most_recent_core_start_date                                                             as most_recent_core_start_date,
        most_recent_core_cancellation_date                                                      as most_recent_core_cancellation_date,
        null                                                                                    as legacy_mindbody_sfdc_account_id,
        legacy_booker_sfdc_account_id                                                           as legacy_booker_sfdc_account_id
    from booker_legacy

)

select * from final
