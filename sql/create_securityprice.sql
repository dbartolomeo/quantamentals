
CREATE TABLE dbo.security_price (
	-- The Intrinio ID for Security  // id	str
	intrinio_id varchar(50)
	-- The calendar date that the stock price represents. For non-daily stock prices, this represents the last day in the period (end of the week, month, quarter, year, etc)  // date	date	
	,asofdate date
	-- If True, the stock price represents an unfinished period (be it day, week, quarter, month, or year), meaning that the close price is the latest price available, not the official close price for the period  // intraperiod	bool	
	,intraperiod bool
	-- The type of period that the stock price represents // frequency	str
	,frequency varchar(50)
	-- The price at the beginning of the period  // open	float	
	,open_px float	
	-- The highest price over the span of the period  // high	float	
	,high_px float	
	-- The lowest price over the span of the period  // low	float	
	,low_px float	
	-- The price at the end of the period  // close	float	
	,close_px float	
	-- The number of shares exchanged during the period  // volume	float	
	,volume float
	-- The price at the beginning of the period, adjusted for splits and dividends  // adj_open	float
	,adj_open float
	-- The highest price over the span of the period, adjusted for splits and dividends  // adj_high	float	
	,adj_high float	
	-- The lowest price over the span of the period, adjusted for splits and dividends  // adj_low	float	
	,adj_low float	
	-- The price at the end of the period, adjusted for splits and dividends  // adj_close	float	
	,adj_close float	
	-- The number of shares exchanged during the period, adjusted for splits and dividends  // adj_volume	float	
	,adj_volume float	
	)
;


CREATE INDEX security_price_intrinio_id_plus_asofdate
ON dbo.security_price (intrinio_id, asofdate);