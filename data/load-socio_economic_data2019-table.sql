-- Format
-- bgid2016,bgid2011,bgid2010,latitude,longitude,tract,st_cnty,STUSAB,state_code,County_Name,CBSA_Code,CBSA_Title,CSA_Code,CSA_Title,Metro_Micro,Urban_Code,Urban_Name,Area_Type,place_code,Place_Name,Area_kmsq,Congressional_District,State_Upper_Legislative_District,State_Lower_Legislative_District,Unified_School_District,Hospital_Service_Area_Code,Hospital_Service_Area_City,Hospital_Service_Area_State,Hospital_Referral_Area_Code,Hospital_Referral_Area_City,Hospital_Referral_Area_State,EstPopulation_2011,EstResidentialDensity_2011,EstMedianHouseholdIncome_2011,EstPropNonHispWhite_2011,EstPropHighSchoolMaxEducation_2011,EstPropNoAuto_2011,EstPropESL_2011,EstPropFemaleHouseholdNoSpouse_2011,EstPropFemaleHouseholdFamilyChild_2011,EstPropFemaleHouseholdAnyChild_2011,EstPropHighSchoolDropout_2011,EstPropHighSchoolDropoutNoWork_2011,EstPropHouseholdSSI_2011,EstPropHouseholdPA_2011,EstPropMaleLittleWork_2011,EstPopulation_2016,EstResidentialDensity_2016,EstMedianHouseholdIncome_2016,EstPropNonHispWhite_2016,EstPropHighSchoolMaxEducation_2016,EstPropNoAuto_2016,EstPropNoHealthIns_2016,EstPropESL_2016,EstPropFemaleHouseholdNoSpouse_2016,EstPropFemaleHouseholdFamilyChild_2016,EstPropFemaleHouseholdAnyChild_2016,EstPropHighSchoolDropout_2016,EstPropHighSchoolDropoutNoWork_2016,EstPropHouseholdSSI_2016,EstPropHouseholdPA_2016,EstPropMaleLittleWork_2016,EstPopulation_2016SE,EstResidentialDensity_2016SE,EstMedianHouseholdIncome_2016SE,EstPropNonHispWhite_2016SE,EstPropHighSchoolMaxEducation_2016SE,EstPropNoAuto_2016SE,EstPropNoHealthIns_2016SE,EstPropESL_2016SE,EstPropFemaleHouseholdNoSpouse_2016SE,EstPropFemaleHouseholdFamilyChild_2016SE,EstPropFemaleHouseholdAnyChild_2016SE,EstPropHouseholdSSI_2016SE,EstPropHouseholdPA_2016SE
-- 010010201001,010010201001,010010201001,32.4658291,-86.4896143,01001020100,01001,al,01,Autauga County,33860,"Montgomery, AL",,,Metropolitan Statistical Area,58600,"Montgomery, AL Urbanized Area",Urbanized Area,62328,Prattville city,4.254524,02,030,088,00240,1070,Prattville,AL,7,Montgomery,AL,499,117.2869162,58077,0.921843687,0.964467005,0.131578947,0,0.118110236,0,0,,,0.210526316,0,0.304347826,745,175.1077206,,0.763758389,0.53164557,0.038732394,0.183892617,0.075070822,0.362694301,0.227979275,0.154929577,,,0.088028169,0,0.385245902,137.5303603,32.32567504,9888,0.105181283,0.076480429,0.039252128,0.063328335,0.040387049,0.108321058,0.151408698,0.075973316,0.04974414,0.060887293

-- create a temporary table for holding the raw data
CREATE TEMP TABLE tmp (
	bgid2016 TEXT,
	bgid2011 TEXT,
	bgid2010 TEXT,
	latitude TEXT,
	longitude TEXT,
	tract TEXT,
	st_cnty TEXT,
	STUSAB TEXT,
	state_code TEXT,
	County_Name TEXT,
	CBSA_Code TEXT,
	CBSA_Title TEXT,
	CSA_Code TEXT,
	CSA_Title TEXT,
	Metro_Micro TEXT,
	Urban_Code TEXT,
	Urban_Name TEXT,
	Area_Type TEXT,
	place_code TEXT,
	Place_Name TEXT,
	Area_kmsq TEXT,
	Congressional_District TEXT,
	State_Upper_Legislative_District TEXT,
	State_Lower_Legislative_District TEXT,
	Unified_School_District TEXT,
	Hospital_Service_Area_Code TEXT,
	Hospital_Service_Area_City TEXT,
	Hospital_Service_Area_State TEXT,
	Hospital_Referral_Area_Code TEXT,
	Hospital_Referral_Area_City TEXT,
	Hospital_Referral_Area_State TEXT,
	EstPopulation_2011 TEXT,
	EstResidentialDensity_2011 TEXT,
	EstMedianHouseholdIncome_2011 TEXT,
	EstPropNonHispWhite_2011 TEXT,
	EstPropHighSchoolMaxEducation_2011 TEXT,
	EstPropNoAuto_2011 TEXT,
	EstPropESL_2011 TEXT,
	EstPropFemaleHouseholdNoSpouse_2011 TEXT,
	EstPropFemaleHouseholdFamilyChild_2011 TEXT,
	EstPropFemaleHouseholdAnyChild_2011 TEXT,
	EstPropHighSchoolDropout_2011 TEXT,
	EstPropHighSchoolDropoutNoWork_2011 TEXT,
	EstPropHouseholdSSI_2011 TEXT,
	EstPropHouseholdPA_2011 TEXT,
	EstPropMaleLittleWork_2011 TEXT,
	EstPopulation_2016 TEXT,
	EstResidentialDensity_2016 TEXT,
	EstMedianHouseholdIncome_2016 TEXT,
	EstPropNonHispWhite_2016 TEXT,
	EstPropHighSchoolMaxEducation_2016 TEXT,
	EstPropNoAuto_2016 TEXT,
	EstPropNoHealthIns_2016 TEXT,
	EstPropESL_2016 TEXT,
	EstPropFemaleHouseholdNoSpouse_2016 TEXT,
	EstPropFemaleHouseholdFamilyChild_2016 TEXT,
	EstPropFemaleHouseholdAnyChild_2016 TEXT,
	EstPropHighSchoolDropout_2016 TEXT,
	EstPropHighSchoolDropoutNoWork_2016 TEXT,
	EstPropHouseholdSSI_2016 TEXT,
	EstPropHouseholdPA_2016 TEXT,
	EstPropMaleLittleWork_2016 TEXT,
	EstPopulation_2016SE TEXT,
	EstResidentialDensity_2016SE TEXT,
	EstMedianHouseholdIncome_2016SE TEXT,
	EstPropNonHispWhite_2016SE TEXT,
	EstPropHighSchoolMaxEducation_2016SE TEXT,
	EstPropNoAuto_2016SE TEXT,
	EstPropNoHealthIns_2016SE TEXT,
	EstPropESL_2016SE TEXT,
	EstPropFemaleHouseholdNoSpouse_2016SE TEXT,
	EstPropFemaleHouseholdFamilyChild_2016SE TEXT,
	EstPropFemaleHouseholdAnyChild_2016SE TEXT,
	EstPropHouseholdSSI_2016SE TEXT,
	EstPropHouseholdPA_2016SE TEXT
);

-- copy the raw data from sample csv file
COPY tmp FROM '/projects/datatrans/ACS_Data/translator_2011_2016_24May2019.csv' DELIMITER ',' CSV HEADER ;

-- create a table to load data into named socio_economic_data_2019
CREATE TABLE IF NOT EXISTS socio_economic_data_2019 (
    id SERIAL UNIQUE PRIMARY KEY,
    bgid2016 TEXT,
    bgid2011 TEXT,
    bgid2010 TEXT,
    latitude FLOAT,
    longitude FLOAT,
    tract TEXT,
    st_cnty TEXT,
    STUSAB TEXT,
    state_code TEXT,
    County_Name TEXT,
    CBSA_Code TEXT,
    CBSA_Title TEXT,
    CSA_Code TEXT,
    CSA_Title TEXT,
    Metro_Micro TEXT,
    Urban_Code TEXT,
    Urban_Name TEXT,
    Area_Type TEXT,
    place_code TEXT,
    Place_Name TEXT,
    Area_kmsq FLOAT,
    Congressional_District TEXT,
    State_Upper_Legislative_District TEXT,
    State_Lower_Legislative_District TEXT,
    Unified_School_District TEXT,
    Hospital_Service_Area_Code TEXT,
    Hospital_Service_Area_City TEXT,
    Hospital_Service_Area_State TEXT,
    Hospital_Referral_Area_Code TEXT,
    Hospital_Referral_Area_City TEXT,
    Hospital_Referral_Area_State TEXT,
    EstPopulation_2011 BIGINT,
    EstPopulation_2016 BIGINT,
    EstPopulation_2016SE FLOAT,
    EstResidentialDensity_2011 FLOAT,
    EstResidentialDensity_2016 FLOAT,
    EstResidentialDensity_2016SE FLOAT,
    EstMedianHouseholdIncome_2011 MONEY,
    EstMedianHouseholdIncome_2016 MONEY,
    EstMedianHouseholdIncome_2016SE FLOAT,
    EstPropNonHispWhite_2011 FLOAT,
    EstPropNonHispWhite_2016 FLOAT,
    EstPropNonHispWhite_2016SE FLOAT,
    EstPropHighSchoolMaxEducation_2011 FLOAT,
    EstPropHighSchoolMaxEducation_2016 FLOAT,
    EstPropHighSchoolMaxEducation_2016SE FLOAT,
    EstPropNoAuto_2011 FLOAT,
    EstPropNoAuto_2016 FLOAT,
    EstPropNoAuto_2016SE FLOAT,
    EstPropESL_2011 FLOAT,
    EstPropESL_2016 FLOAT,
    EstPropESL_2016SE FLOAT,
    EstPropNoHealthIns_2016 FLOAT,
    EstPropNoHealthIns_2016SE FLOAT,
    EstPropFemaleHouseholdNoSpouse_2011 FLOAT,
    EstPropFemaleHouseholdNoSpouse_2016 FLOAT,
    EstPropFemaleHouseholdNoSpouse_2016SE FLOAT,
    EstPropFemaleHouseholdFamilyChild_2011 FLOAT,
    EstPropFemaleHouseholdFamilyChild_2016 FLOAT,
    EstPropFemaleHouseholdFamilyChild_2016SE FLOAT,
    EstPropFemaleHouseholdAnyChild_2011 FLOAT,
    EstPropFemaleHouseholdAnyChild_2016 FLOAT,
    EstPropFemaleHouseholdAnyChild_2016SE FLOAT,
    EstPropHighSchoolDropout_2011 FLOAT,
    EstPropHighSchoolDropout_2016 FLOAT,
    EstPropHighSchoolDropoutNoWork_2011 FLOAT,
    EstPropHighSchoolDropoutNoWork_2016 FLOAT,
    EstPropHouseholdSSI_2011 FLOAT,
    EstPropHouseholdSSI_2016 FLOAT,
    EstPropHouseholdSSI_2016SE FLOAT,
    EstPropHouseholdPA_2011 FLOAT,
    EstPropHouseholdPA_2016 FLOAT,
    EstPropHouseholdPA_2016SE FLOAT,
    EstPropMaleLittleWork_2011 FLOAT,
    EstPropMaleLittleWork_2016 FLOAT
);

-- load the socio_economic_data_2019 table with properly formatted data
INSERT INTO socio_economic_data_2019 (bgid2016, bgid2011, bgid2010, latitude, longitude, tract, st_cnty, STUSAB, state_code, County_Name, CBSA_Code, CBSA_Title, CSA_Code, CSA_Title, Metro_Micro, Urban_Code, Urban_Name, Area_Type, place_code, Place_Name, Area_kmsq, Congressional_District, State_Upper_Legislative_District, State_Lower_Legislative_District, Unified_School_District, Hospital_Service_Area_Code, Hospital_Service_Area_City, Hospital_Service_Area_State, Hospital_Referral_Area_Code, Hospital_Referral_Area_City, Hospital_Referral_Area_State, EstPopulation_2011, EstPopulation_2016, EstPopulation_2016SE, EstResidentialDensity_2011, EstResidentialDensity_2016, EstResidentialDensity_2016SE, EstMedianHouseholdIncome_2011, EstMedianHouseholdIncome_2016, EstMedianHouseholdIncome_2016SE, EstPropNonHispWhite_2011, EstPropNonHispWhite_2016, EstPropNonHispWhite_2016SE, EstPropHighSchoolMaxEducation_2011, EstPropHighSchoolMaxEducation_2016, EstPropHighSchoolMaxEducation_2016SE, EstPropNoAuto_2011, EstPropNoAuto_2016, EstPropNoAuto_2016SE, EstPropESL_2011, EstPropESL_2016, EstPropESL_2016SE, EstPropNoHealthIns_2016, EstPropNoHealthIns_2016SE, EstPropFemaleHouseholdNoSpouse_2011, EstPropFemaleHouseholdNoSpouse_2016, EstPropFemaleHouseholdNoSpouse_2016SE, EstPropFemaleHouseholdFamilyChild_2011, EstPropFemaleHouseholdFamilyChild_2016, EstPropFemaleHouseholdFamilyChild_2016SE, EstPropFemaleHouseholdAnyChild_2011, EstPropFemaleHouseholdAnyChild_2016, EstPropFemaleHouseholdAnyChild_2016SE, EstPropHighSchoolDropout_2011, EstPropHighSchoolDropout_2016, EstPropHighSchoolDropoutNoWork_2011, EstPropHighSchoolDropoutNoWork_2016, EstPropHouseholdSSI_2011, EstPropHouseholdSSI_2016, EstPropHouseholdSSI_2016SE, EstPropHouseholdPA_2011, EstPropHouseholdPA_2016, EstPropHouseholdPA_2016SE, EstPropMaleLittleWork_2011, EstPropMaleLittleWork_2016)
    SELECT

      bgid2016,
      bgid2011,
      bgid2010,
      cast(latitude as FLOAT),
      cast(longitude as FLOAT),
      tract,
      st_cnty,
      STUSAB,
      state_code,
      County_Name,
      CBSA_Code,
      CBSA_Title,
      CSA_Code,
      CSA_Title,
      Metro_Micro,
      Urban_Code,
      Urban_Name,
      Area_Type,
      place_code,
      Place_Name,
      cast(Area_kmsq as FLOAT),
      Congressional_District,
      State_Upper_Legislative_District,
      State_Lower_Legislative_District,
      Unified_School_District,
      Hospital_Service_Area_Code,
      Hospital_Service_Area_City,
      Hospital_Service_Area_State,
      Hospital_Referral_Area_Code,
      Hospital_Referral_Area_City,
      Hospital_Referral_Area_State,
      cast(EstPopulation_2011 as BIGINT),
      cast(EstPopulation_2016 as BIGINT),
      cast(EstPopulation_2016SE as FLOAT),
      cast(EstResidentialDensity_2011 as FLOAT),
      cast(EstResidentialDensity_2016 as FLOAT),
      cast(EstResidentialDensity_2016SE as FLOAT),
      cast(EstMedianHouseholdIncome_2011 as MONEY),
      cast(EstMedianHouseholdIncome_2016 as MONEY),
      cast(EstMedianHouseholdIncome_2016SE as FLOAT),
      cast(EstPropNonHispWhite_2011 as FLOAT),
      cast(EstPropNonHispWhite_2016 as FLOAT),
      cast(EstPropNonHispWhite_2016SE as FLOAT),
      cast(EstPropHighSchoolMaxEducation_2011 as FLOAT),
      cast(EstPropHighSchoolMaxEducation_2016 as FLOAT),
      cast(EstPropHighSchoolMaxEducation_2016SE as FLOAT),
      cast(EstPropNoAuto_2011 as FLOAT),
      cast(EstPropNoAuto_2016 as FLOAT),
      cast(EstPropNoAuto_2016SE as FLOAT),
      cast(EstPropESL_2011 as FLOAT),
      cast(EstPropESL_2016 as FLOAT),
      cast(EstPropESL_2016SE as FLOAT),
      cast(EstPropNoHealthIns_2016 as FLOAT),
      cast(EstPropNoHealthIns_2016SE as FLOAT),
      cast(EstPropFemaleHouseholdNoSpouse_2011 as FLOAT),
      cast(EstPropFemaleHouseholdNoSpouse_2016 as FLOAT),
      cast(EstPropFemaleHouseholdNoSpouse_2016SE as FLOAT),
      cast(EstPropFemaleHouseholdFamilyChild_2011 as FLOAT),
      cast(EstPropFemaleHouseholdFamilyChild_2016 as FLOAT),
      cast(EstPropFemaleHouseholdFamilyChild_2016SE as FLOAT),
      cast(EstPropFemaleHouseholdAnyChild_2011 as FLOAT),
      cast(EstPropFemaleHouseholdAnyChild_2016 as FLOAT),
      cast(EstPropFemaleHouseholdAnyChild_2016SE as FLOAT),
      cast(EstPropHighSchoolDropout_2011 as FLOAT),
      cast(EstPropHighSchoolDropout_2016 as FLOAT),
      cast(EstPropHighSchoolDropoutNoWork_2011 as FLOAT),
      cast(EstPropHighSchoolDropoutNoWork_2016 as FLOAT),
      cast(EstPropHouseholdSSI_2011 as FLOAT),
      cast(EstPropHouseholdSSI_2016 as FLOAT),
      cast(EstPropHouseholdSSI_2016SE as FLOAT),
      cast(EstPropHouseholdPA_2011 as FLOAT),
      cast(EstPropHouseholdPA_2016 as FLOAT),
      cast(EstPropHouseholdPA_2016SE as FLOAT),
      cast(EstPropMaleLittleWork_2011 as FLOAT),
      cast(EstPropMaleLittleWork_2016 as FLOAT)

    FROM tmp;

-- drop the temporary table
DROP TABLE tmp;

-- set owner to datatrans user
ALTER TABLE socio_economic_data_2019 OWNER TO datatrans;

-- display a sample of contents to user
SELECT * FROM socio_economic_data_2019 ORDER BY id ASC LIMIT 10;
