
CREATE TABLE dbo.security_list (
	-- The Intrinio ID for Security  // id	str
	intrinio_id varchar(50)
	-- The Intrinio ID for the Company for which the Security is issued  // company_id	str
	,company_id varchar(50)
	-- The name of the Security   // name	str
	,company varchar(250)
	-- A 2-3 digit code classifying the Security (reference) // code	str
	,code varchar(50)
	-- The currency in which the Security is traded on the exchange  // currency	str
	,currency varchar(50)
	-- The common/local ticker of the Security  // ticker	str	
	,ticker varchar(50)
	-- The country-composite ticker of the Security  //  composite_ticker	str
	,composite_ticker varchar(50)
	-- The OpenFIGI identifier  // figi	str
	,figi varchar(50)
	-- The country-composite OpenFIGI identifier  // composite_figi	str	
	,composite_figi	varchar(50)
	-- The global-composite OpenFIGI identifier  //  share_class_figi	str	
	,share_class_figi varchar(50)
	-- insert date
	,insert_date date
	-- update date
	,update_date date
	-- current flag
	,is_current int
	)
;

CREATE INDEX security_list_intrinio_id_plus_is_current
ON dbo.security_list (intrinio_id, is_current);

