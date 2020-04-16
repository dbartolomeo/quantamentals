CREATE TABLE dbo.company_details (
	-- The Intrinio ID of the company // id	str
	intrinio_id varchar(50)
	-- The stock market ticker symbol associated with the company's common stock securities  // ticker	str	
	,ticker varchar(50)
	-- The company's common name  // name	str
	,company varchar(250)
	-- The Legal Entity Identifier (LEI) assigned to the company  // lei	str
	,lei varchar(50)
	-- The Central Index Key (CIK) assigned to the company    // cik	str	
	,legal_name varchar(250)
	-- The Stock Exchange where the company's common stock is primarily traded   // stock_exchange	str	
	,stock_exchange varchar(50)
	-- The Standard Industrial Classification (SIC) determined by the company and filed with the SEC  // sic	str
	,sic varchar(50)
	-- A one or two sentence description of the company's operations  // short_description str
	,short_description varchar(25000)
	-- A one paragraph description of the company's operations and other corporate actions   // long_description str
	,long_description varchar(50000)
	-- The Chief Executive Officer of the company  // ceo	str
	,ceo varchar(50)
	-- The URL of the company's primary corporate website or primary internet property  // company_url	str
	,company_url varchar(1000)
	-- The company's business address  // business_address str	
	,business_address varchar(1000)
	-- The mailing address reported by the company  // mailing_address	str
	,mailing_address varchar(1000)
	-- The phone number reported by the company  // business_phone_no	str	
	,business_phone_no varchar(50)
	-- The company's headquarters address - line 1  // hq_address1	str	
	,hq_address1 varchar(1000)
	-- The company's headquarters address - line 2  // hq_address2	str	
	,hq_address2 varchar(1000)
	-- The company's headquarters city   // hq_address_city	str	
	,hq_address_city varchar(250)
	-- The company's headquarters postal code    // hq_address_postal_code	str	
	,hq_address_postal_code varchar(50)
	-- The company's legal organization form  // entity_legal_form	str	
	,entity_legal_form varchar(250)
	-- The Central Index Key (CIK) assigned to the company by the SEC as a unique identifier, used in SEC filings  // cik	str	
	,cik varchar(50)
	-- The date of the company's last filing with the SEC  // latest_filing_date	date	
	,latest_filing_date date
	-- The state (US & Canada Only) where the company headquarters are located    // hq_state	str	
	,hq_state varchar(50)
	-- The country where the company headquarters are located    // hq_country	str	
	,hq_country varchar(50)
	-- The state (US & Canada Only) where the company is incorporated  // inc_state	str	
	,inc_state varchar(50)
	-- The country where the company is incorporated    // inc_country	str	
	,inc_country varchar(50)
	-- The number of employees working for the company // employees	int	
	,employees	int
	-- entity_status	str
	,entity_status varchar(50)
	-- The company's operating sector  // sector	str
	,sector	varchar(250)
	-- The company's operating industry category  // industry_category	str	
	,industry_category	varchar(250)
	-- The company's operating industry group  // industry_group	str	
	,industry_group	varchar(250)
	-- The financial statement template used by Intrinio to standardize the as reported data  // template	str	
	,finstate_template varchar(250)	
	-- If True, the company has standardized and as reported fundamental data via the Intrinio API; if False, the company has as-reported data only  // standardized_active	bool	
	,standardized_active	bool	
	-- The period end date of the company's first reported fundamental // first_fundamental_date	date	
	,first_fundamental_date	date	
	-- The period end date of the company's last reported fundamental // last_fundamental_date	date	
	,last_fundamental_date date	
	-- The date of the company's first stock price, based on the company's primary security, which is typically traded on US exchages  //  first_stock_price_date	date	
	,first_stock_price_date	date	
	-- The date of the company's last stock price, based on the company's primary security, which is typically traded on US exchages   // last_stock_price_date	date	
	,last_stock_price_date date	
	-- insert date
	,insert_date date
	-- update date
	,update_date date
	-- current flag
	,is_current bit
	)

;

CREATE INDEX intrinio_id_plus_is_current
ON dbo.company_details (intrinio_id, is_current);



