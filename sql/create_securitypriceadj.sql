
CREATE TABLE dbo.security_priceadj (
	-- The Intrinio ID for Security  // id	str
	intrinio_id varchar(50)
	-- The date on which the adjustment occurred. The adjustment should be applied to all stock prices before this date.    // date	date	
	,asofdate date
	-- The factor by which to multiply stock prices before this date, in order to calculate historically-adjusted stock prices.  // factor	float	
	,factor float	
	-- The dividend amount, if a dividend was paid.  // dividend	float	
	,dividend float	
	-- The currency of the dividend, if known.  // dividend_currency	str	
	,dividend_currency varchar(50)	
	-- The ratio of the stock split, if a stock split occurred.  // split_ratio	float	
	,split_ratio float
	)
;

CREATE INDEX security_pricesdj_intrinio_id_plus_asofdate
ON dbo.security_priceadj (intrinio_id, asofdate);

