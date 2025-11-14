{{ 
    config(
      materialized='incremental',
      unique_key='unique_id',
      on_schema_change='sync',
      alias='NFT_WASH_TRADES'
    ) 
}}

{% set w1 = 4 %}
{% set w2 = 2 %}
{% set w3 = 1 %}
{% set w4 = 1 %}
{% set threshold = 3 %}

WITH t AS (
  SELECT 
    * 
   FROM {{ ref('nft_trades') }}
),
bs AS (select * from {{ ref('buyer_is_seller') }}),
baf_t AS (select * from {{ ref('baf_token_24h') }}),
baf_c AS (select * from {{ref('baf_collection_24h') }}),
nft AS (select * from {{ ref('same_nft_traded_7d') }}),

joined AS (
  SELECT
    t.MARKETPLACE,
    t.BUYER_ADDRESS,
    t.SELLER_ADDRESS,
    t.TOKEN_ADDRESS,
    t.TOKEN_ID,
    t.TOKEN_NAME,
    t.TOKEN_SYMBOL,
    t.PRICE,
    t.CURRENCY_ADDRESS,
    t.USD_PRICE,
    t.BLOCK_TIMESTAMP,
    t.TRANSACTION_HASH,
    t.UNIQUE_ID,
    COALESCE(bs.buyer_is_seller, FALSE) AS buyer_is_seller,
    COALESCE(baf_t.baf_token_24h, FALSE) AS baf_token_24h,
    COALESCE(baf_c.baf_collection_24h, FALSE) AS baf_collection_24h,
    COALESCE(nft.same_nft_traded_7d, FALSE) AS same_nft_traded_7d
  FROM t
  LEFT JOIN 
    bs 
    USING (UNIQUE_ID)
  LEFT JOIN 
    baf_t 
    USING (UNIQUE_ID)
  LEFT JOIN 
    baf_c 
    USING (UNIQUE_ID)
  LEFT JOIN 
    nft 
    USING (UNIQUE_ID)
  
{% if is_incremental() %}
  WHERE t.BLOCK_TIMESTAMP >= DATEADD(day,-14,(select MAX(BLOCK_TIMESTAMP) from {{ this }}))
{% endif %}

)

SELECT
  TOKEN_ADDRESS,
  TOKEN_ID,
  TOKEN_NAME,
  TOKEN_SYMBOL,
  BLOCK_TIMESTAMP,
  TRANSACTION_HASH,
  UNIQUE_ID,
  MARKETPLACE,
  BUYER_ADDRESS,
  SELLER_ADDRESS,
  PRICE,
  USD_PRICE,
  CURRENCY_ADDRESS,
  buyer_is_seller,
  baf_token_24h,
  baf_collection_24h,
  same_nft_traded_7d,

  (
    {{ w1 }} * IFF(buyer_is_seller, 1, 0) +
    {{ w2 }} * IFF(baf_token_24h, 1, 0) +
    {{ w3 }} * IFF(baf_collection_24h, 1, 0) +
    {{ w4 }} * IFF(same_nft_traded_7d, 1, 0)
  ) AS wash_trading_score,

  (
    (
      {{ w1 }} * IFF(buyer_is_seller, 1, 0) +
      {{ w2 }} * IFF(baf_token_24h, 1, 0) +
      {{ w3 }} * IFF(baf_collection_24h, 1, 0) +
      {{ w4 }} * IFF(same_nft_traded_7d, 1, 0)
    ) >= {{ threshold }}
  ) AS is_wash_trading

FROM joined


