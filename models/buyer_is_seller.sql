{{
  config(
    materialized = 'incremental',
    unique_key = 'unique_id',
    alias = 'BUYER_IS_SELLER'
  )
}}

WITH t AS (
  SELECT
    UNIQUE_ID,
    BUYER_ADDRESS,
    SELLER_ADDRESS,
    BLOCK_TIMESTAMP
  FROM 
    {{ ref('nft_trades') }}
    
  {% if is_incremental() %}
  WHERE BLOCK_TIMESTAMP >= DATEADD(day,-14,(select MAX(BLOCK_TIMESTAMP) from {{ ref('nft_trades') }}))
  {% endif %}
)

SELECT
  UNIQUE_ID,
  (BUYER_ADDRESS = SELLER_ADDRESS) AS buyer_is_seller
FROM t