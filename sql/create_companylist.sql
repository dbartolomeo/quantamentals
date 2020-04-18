CREATE TABLE dbo.company_list (
	-- The Intrinio ID of the company // id	str
	company_id varchar(50)
	-- The stock market ticker symbol associated with the company's common stock securities  // ticker	str	
	,ticker varchar(50)
	-- The company's common name  // name	str
	,company varchar(250)
	-- The Legal Entity Identifier (LEI) assigned to the company  // lei	str
	,lei varchar(50)
	-- The Central Index Key (CIK) assigned to the company    // cik	str	
	,cik varchar(50)
	-- insert date
	,insert_date date
	-- update date
	,update_date date
	-- current flag
	,is_current int
	)
;

CREATE INDEX company_list_company_id_plus_is_current
ON dbo.company_list (company_id, is_current);



