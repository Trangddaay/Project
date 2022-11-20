-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT 
   format_date("%Y%m", parse_date("%Y%m%d", date))  month,
   sum(totals.visits)  visits,
   sum(totals.pageviews) pageviews,
   sum(totals.transactions) transactions,
   sum(totals.totalTransactionRevenue)/1000000 revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101'and '20170331'
group by 1
order by 1 

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with purchase as(
SELECT
   format_date("%Y%m", parse_date("%Y%m%d", date)) month,  
   fullVisitorId,
   sum(totals.pageviews) total_pageviews
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0601' and '0731'
and totals.transactions >=1
group by 1,2
order by 1),

non_purchase as (
select
   format_date("%Y%m", parse_date("%Y%m%d", date)) month1,
   fullVisitorId,
   sum(totals.pageviews) total_pageview2
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0601' and '0731'
and totals.transactions is null
group by 1,2
order by 1)

select
   month,
   sum(total_pageviews)/count(purchase.fullVisitorId) as avg_pageviews_purchase,
   sum(non_purchase.total_pageview2)/count(non_purchase.fullVisitorId) avg_pageviews_non_purchase
from purchase
left join non_purchase
on purchase.month = non_purchase.month1
where month = '201706' or month = '201707' 
group by 1
order by 1

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

with transactions as(
select  
   format_date("%Y%m", parse_date("%Y%m%d", date)) month,
   fullVisitorId,
   sum(totals.transactions) total_transactions
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >=1 
group by 1,2
order by 1)

select
   month,
   sum(total_transactions)/count(fullVisitorId) avg_total_transactions_per_user
from transactions
group by 1

--
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month


-- Query 06: Average amount of money spent per session
#standardSQL

select
   format_date("%Y%m", parse_date("%Y%m%d", date)) month,
   sum(totals.totalTransactionRevenue)/sum(totals.visits) avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0701' and '0731'
and totals.transactions is not null
group by 1

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

with product_name as(
SELECT  
   fullVisitorId userId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) hits,
unnest(hits.product) product 
where product.v2ProductName = "YouTube Men's Vintage Henley"
and product.productRevenue is not Null)

select
   product.v2ProductName other_purchased_products,
   count(product.productQuantity) quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` bigquery,
unnest(hits) hits,
unnest(hits.product) product
inner join product_name on bigquery.fullVisitorId = product_name.userId 
where product.productRevenue is not null
and product.v2ProductName <> "YouTube Men's Vintage Henley"
group by 1
order by 2 desc

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data
