CREATE TABLE IF NOT EXISTS ethereum.nfts.nft_trades (
  marketplace VARCHAR, 
  buyer_address VARCHAR(42),
  seller_address VARCHAR(42),
  token_address VARCHAR(42), 
  token_id VARCHAR,
  token_name VARCHAR,
  token_symbol VARCHAR,
  price FLOAT,
  currency_address VARCHAR(42), 
  usd_price FLOAT, 
  block_timestamp TIMESTAMP_NTZ(9),
  transaction_hash VARCHAR,
  unique_id VARCHAR
)
