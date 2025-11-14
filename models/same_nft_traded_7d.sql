{{ 
    config(
      materialized = 'incremental',
      unique_key = 'unique_id',
      alias = 'SAME_NFT_TRADED_7D'
    ) 
}}

WITH t AS (
  SELECT
    TOKEN_ADDRESS,
    TOKEN_ID,
    BLOCK_TIMESTAMP,
    UNIQUE_ID,
    BUYER_ADDRESS,
    SELLER_ADDRESS
  FROM 
    {{ ref('nft_trades') }}
    
  {% if is_incremental() %}
  WHERE BLOCK_TIMESTAMP >= DATEADD(day,-14,(SELECT MAX(BLOCK_TIMESTAMP) FROM {{ ref('nft_trades') }}))
  {% endif %}
),

addr_events AS (
  SELECT
    TOKEN_ADDRESS,
    TOKEN_ID,
    BLOCK_TIMESTAMP,
    UNIQUE_ID,
    BUYER_ADDRESS AS addr,
    'BUY' AS side
  FROM t

  UNION ALL

  SELECT
    TOKEN_ADDRESS,
    TOKEN_ID,
    BLOCK_TIMESTAMP,
    UNIQUE_ID,
    SELLER_ADDRESS AS addr,
    'SELL' AS side
  FROM t
),

w AS (
  SELECT
    TOKEN_ADDRESS,
    TOKEN_ID,
    UNIQUE_ID,
    addr,
    side,
    BLOCK_TIMESTAMP,

    SUM(IFF(side = 'BUY', 1, 0)) OVER (
      PARTITION BY TOKEN_ADDRESS, TOKEN_ID, addr
      ORDER BY BLOCK_TIMESTAMP
      RANGE BETWEEN INTERVAL '7 day' PRECEDING AND CURRENT ROW
    ) 
    AS buys_in_win,

    SUM(IFF(side = 'SELL', 1, 0)) OVER (
      PARTITION BY TOKEN_ADDRESS, TOKEN_ID, addr
      ORDER BY BLOCK_TIMESTAMP
      RANGE BETWEEN INTERVAL '7 day' PRECEDING AND CURRENT ROW
    ) 
    AS sells_in_win,

    COUNT(*) OVER (
      PARTITION BY TOKEN_ADDRESS, TOKEN_ID, addr
      ORDER BY BLOCK_TIMESTAMP
      RANGE BETWEEN INTERVAL '7 day' PRECEDING AND CURRENT ROW
    ) 
    AS trades_in_win
  FROM addr_events
),

flags AS (
  SELECT
    UNIQUE_ID,
    IFF(trades_in_win >= 3 AND buys_in_win > 0 AND sells_in_win > 0, TRUE, FALSE) AS loop_flag
  FROM w
)

SELECT
  UNIQUE_ID,
  MAX(loop_flag) AS same_nft_traded_7d
FROM 
    flags
GROUP BY UNIQUE_ID

{% if is_incremental() %}
HAVING MAX(loop_flag) IS NOT NULL
{% endif %}
