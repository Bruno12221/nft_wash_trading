
{{ 
config(
  materialized = 'incremental',
  unique_key = 'unique_id',
  on_schema_change = 'sync',
  alias = 'NFT_TRADES'
) 
}}

WITH 
base AS (
  SELECT
    MARKETPLACE,
    BUYER_ADDRESS,
    SELLER_ADDRESS,
    TOKEN_ADDRESS,
    TOKEN_ID,
    TOKEN_NAME,
    TOKEN_SYMBOL,
    PRICE,
    CURRENCY_ADDRESS,
    USD_PRICE,
    BLOCK_TIMESTAMP,
    TRANSACTION_HASH,
    UNIQUE_ID
  FROM 
    {{ source('ethereum_nfts', 'TRADES') }}
  WHERE 
    CURRENCY_SYMBOL = 'USDC'
  QUALIFY ROW_NUMBER() OVER (partition by UNIQUE_ID order by BLOCK_TIMESTAMP) = 1
)

SELECT *
FROM base
    
{% if is_incremental() %}
WHERE BLOCK_TIMESTAMP >= DATEADD(day,-14,(select MAX(BLOCK_TIMESTAMP) 
FROM {{ this }}))

{% endif %}

