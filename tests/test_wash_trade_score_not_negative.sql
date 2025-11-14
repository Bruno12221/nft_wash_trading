--detects if there are any negative wash trading scores in nft_wash_trades
select 
    *
from 
    {{ ref('nft_wash_trades') }}
where 
    wash_trading_score < 0