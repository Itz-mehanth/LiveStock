DROP TABLE IF EXISTS daily_stock_agg;

CREATE TABLE daily_stock_agg (
    symbol VARCHAR(10),
    date TIMESTAMP WITH TIME ZONE,
    avg_price DOUBLE PRECISION,
    PRIMARY KEY (symbol, date)
);