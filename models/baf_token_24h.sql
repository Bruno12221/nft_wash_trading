{{ 
    config(
      materialized = 'incremental',
      unique_key = 'unique_id',
      alias = 'BAF_TOKEN_24H'
    ) 
}}

WITH t AS (
  SELECT
    TOKEN_ADDRESS,
    TOKEN_ID,
    BLOCK_TIMESTAMP,
    BUYER_ADDRESS,
    SELLER_ADDRESS,
    UNIQUE_ID
  FROM 
    {{ ref('nft_trades') }}
    
  {% if is_incremental() %}
  WHERE BLOCK_TIMESTAMP >= DATEADD(day,-14,(SELECT MAX(BLOCK_TIMESTAMP) FROM {{ ref('nft_trades') }}))
  {% endif %}
),

seq AS (
  SELECT
    t.*,
    LAG(BUYER_ADDRESS) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS prev_buyer,
    LAG(SELLER_ADDRESS) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS prev_seller,
    LAG(BLOCK_TIMESTAMP) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS prev_timestamp,
    LEAD(BUYER_ADDRESS) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS next_buyer,
    LEAD(SELLER_ADDRESS) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS next_seller,
    LEAD(BLOCK_TIMESTAMP) OVER (PARTITION BY TOKEN_ADDRESS, TOKEN_ID ORDER BY BLOCK_TIMESTAMP, UNIQUE_ID) AS next_timestamp
  FROM t
)

SELECT
  UNIQUE_ID,
  (
    (
      prev_timestamp IS NOT NULL
      AND DATEDIFF('hour', prev_timestamp, BLOCK_TIMESTAMP) <= 24
      AND prev_buyer = SELLER_ADDRESS
      AND prev_seller = BUYER_ADDRESS
    )
    OR
    (
      next_timestamp IS NOT NULL
      AND DATEDIFF('hour', BLOCK_TIMESTAMP, next_timestamp) <= 24
      AND next_buyer = SELLER_ADDRESS
      AND next_seller = BUYER_ADDRESS
      )
  ) AS baf_token_24h
FROM seq



