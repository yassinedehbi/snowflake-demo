/* ------------------------------------------------------------------------------
   File Name: V1.1.1__initial_objects.sql
   Description: 
       This SQL script sets up the initial objects required for the DataVault Architecture, 
       including stages - raw staging tables within the "staging" schema. 
       The tables and stages are designed to ingest CSV-formatted data for 
       "product", "location", and "sales" datasets in preparation for further 
       transformation and processing in a Data Vault model.

   Schema: 
       staging

   Objects Created:
       1. Stages:
           - product_data
           - location_data
           - sales_data

       2. Pipes :
           - STG_LOCATION_PP / STG_PRODUCT_PP / STG_SALES_PP

       3. Tables:
           - STG_LOCATION_RAW / STG_PRODUCT_RAW / STG_SALES_RAW
           - STG_LOCATION / STG_PRODUCT / STG_SALES   (after cleansing)
        
        4. Streams :
           - STG_LOCATION_STRM / STG_PRODUCT_STRM / STG_SALES_STRM (Keeps track of changes in STG_X_RAW tables)

        5. Tasks:
           - STG_LOCATION_TSK / STG_PRODUCT_TSK / STG_SALES_TSK  (move data from tables STG_X_RAW to STG_X based on a stream)
        
        6. Views:
           - STG_LOCATION_VIEW / STG_PRODUCT_VIEW / STG_SALES_VIEW  (Reference streams present in the raw_dtv schema)

    Schema: 
       raw_dtv

   Objects Created:

       1. Tables:
           - HUB_LOCATION
           - HUB_PRODUCT
           - HUB_SALES
           - SAT_LOCATION
           - SAT_PRODUCT
           - SAT_SALES
           - LNK_PRODUCT_SALES
           - LNK_LOCATION_SALES

        2. Streams :
           - LOCATION_OUTBOUND_STRM / PRODUCT_OUTBOUND_STRM / SALES_OUTBOUND_STRM (Keeps track of changes in STG_X tables)
        
        3. Tasks :
           - LOCATION_STRM_TSK / PRODUCT_STRM_TSK / SALES_STRM_TSK ( Tasks to orchestrate movement from staging to raw_dtv , se basant sur les vues dans STAGING)



   Author:  M'hamed Issam ED-DAOU && Yassine DEHBI  -- VISEO
------------------------------------------------------------------------------- */


-------------------------------- LANDING / Staging schema ----------------------------------------
use schema staging;

 -------------------------------------------- Create stages ---------------------
create stage if not exists product_data file_format = (type = CSV skip_header = 1 ) DIRECTORY = ( ENABLE = true );

create stage if not exists location_data file_format = (type = CSV skip_header = 1 FIELD_DELIMITER = ',') DIRECTORY = ( ENABLE = true );

create stage if not exists sales_data file_format = (type = CSV skip_header = 1 FIELD_DELIMITER = ',') DIRECTORY = ( ENABLE = true );


 -------------------------------------------- Create staging RAW Tables ---------------------
CREATE or replace TABLE staging.STG_LOCATION_RAW (
    LOCATION_CODE varchar,
    LOCATION_ID varchar,
    LOCATION_PLANNING_CODE varchar,
    CANCELLED_FLAG varchar,
    DUMMY_FLAG varchar,
    STORE_GROUP_CODE varchar, --timestamp
    CURRENCY_ID varchar,
    CONCEPT_CODE varchar,
    SELL_SQUARE_FEET varchar, --TIMESTAMP
    SELL_MQ varchar,
    NOSELL_SQUARE_FEET varchar,
    NOSELL_MQ varchar,
    GROSS_STORE_SIZE varchar,
    SDB_CHANNEL_CODE varchar,
    LOCATION_SUBSUBTYPE_ID varchar,
    LOCATION_SUBSUBTYPE_CODE varchar,
    LOCATION_SUBSUBTYPE_TYPOLOGY varchar,
    LOCATION_SUBTYPE_ID varchar,
    LOCATION_SUBTYPE_CODE varchar,
    LOCATION_SUBTYPE_CODE_TYPOLOGY varchar,
    LOCATION_TYPE_ID varchar,
    LOCATION_TYPE_CODE varchar,
    LOCATION_TYPE_CODE_TYPOLOGY varchar,
    DISTRIBUTION_CHANNEL_ID varchar,
    DISTRIBUTION_CHANNEL_CODE varchar,
    DISTRIBUTION_CHANNEL_TYPOLOGY varchar,
    CITY_ID number(38, 0),
    CITY_CODE varchar,
    STATE_ID varchar, --TIMESTAMP
    STATE_CODE varchar,--TIMESTAMP
    COUNTRY_ID varchar,
    COUNTRY_CODE varchar,
    DISTRICT_ID varchar,
    DISTRICT_CODE varchar,
    ZONE_ID varchar, --TIMESTAMP
    ZONE_CODE varchar, -- varchar
    REGION_ID varchar,
    REGION_CODE varchar,
    AREA_ID number(38, 0),
    AREA_CODE varchar,
    MACROAREA_ID varchar,
    MACROAREA_CODE varchar,
    LOCATION_STATUS_ID varchar,
    LOCATION_STATUS_CODE varchar,
    REPORTING_UNIT_ID varchar, --TIMESTAMP
    REPORTING_UNIT_CODE varchar,
    MACROZONE_ID varchar,
    MACROZONE_CODE varchar,
    CHANNEL_ID number(38, 0),
    CHANNEL_CODE varchar,
    STORE_GROUP_PLAN_ID varchar, -- TIMESTAMP
    STORE_GROUP_PLAN_CODE varchar, --TIMESTAMP
    STORE_GROUP_PLAN_TYPOLOGY varchar, --TIMESTAMP
    CHANNEL_PLAN_ID  number(38, 0),
    CHANNEL_PLAN_CODE varchar,
    CHANNEL_PLAN_TYPOLOGY varchar,
    COUNTRY_PLAN_ID varchar,
    COUNTRY_PLAN_CODE varchar,
    LOCATION_OPENING_DATE varchar,
    LOCATION_CLOSURE_DATE varchar,
    CONSOLIDATED_FLAG_ID varchar,
    CONSOLIDATED_FLAG_CODE varchar,
    TIER_CITY_ID varchar, --TIMESTAMP
    TIER_CITY_CODE varchar, -- TIMESTAMP
    CHANNEL_PLAN_SORT number(38, 0),
    OPERATION_FLAG varchar,
    INSERT_DATE timestamp,
    UPDATE_DATE TIMESTAMP,
    __DELETE_FLAG BOOLEAN,
    __DQ_FLAG BOOLEAN,
    __DUMMY_FLAG BOOLEAN,
    DATE_PARTITION varchar,
    ldts timestamp,
    source_file varchar
);


create or replace table staging.stg_product_raw (
SKU_ID number(38, 0)  COMMENT 'Product + size key',
SIZE_CODE_REF varchar,
SIZE_CODE_DESC varchar,
SELLING_FLAG varchar,
STYLE_COLOR_ID number(38, 0),
STYLE_COLOR_CODE varchar  COMMENT 'Product code (style + material + color)',
STYLE_COLOR_ORIGINAL_CODE varchar,
STYLE_COLOR_RENAMED_CODE varchar,
STYLE_COLOR_BORN_SEA_EVENT_ID number(38, 0),
STYLE_COLOR_BORN_SEA_YEAR_ID number(38, 0),
STYLE_COLOR_BORN_SEA_YEAR_CODE varchar,
STYLE_COLOR_BORN_EVENT_CODE varchar,
STYLE_COLOR_END_SEA_EVENT_ID number(38, 0),
STYLE_COLOR_END_SEA_YEAR_ID number(38, 0),
STYLE_COLOR_END_SEA_YEAR_CODE varchar,
STYLE_COLOR_END_EVENT_CODE varchar,
STYLE_ID number(38, 0),
STYLE_CODE varchar  COMMENT 'Product code (style + material)',
MODEL_ID number(38, 0),
MODEL_CODE varchar  COMMENT 'Model code (style)',
MODEL_BORN_SEASON_ID number(38, 0),
MODEL_BORN_SEASON_YEAR_CODE varchar,
MATERIAL_ID number(38, 0),
MATERIAL_CODE varchar,
MATERIAL_TRIM_TYPE_ID number(38, 0),
MATERIAL_TRIM_TYPE_CODE varchar,
COLOR_ID number(38, 0),
COLOR_CODE varchar  COMMENT 'Color code',
COLOR_SUBFAMILY_ID number(38, 0),
COLOR_SUBFAMILY_CODE varchar,
COLOR_FAMILY_ID number(38, 0),
COLOR_FAMILY_CODE varchar,
SUBCLASS_ID number(38, 0),
SUBCLASS_CODE varchar  COMMENT 'Subclass code',
SUBCLASS_SHORT_CODE varchar,
SUBCLASS_GROUP_ID number(38, 0),
SUBCLASS_GROUP_CODE number(38, 0)  COMMENT 'Subclass group code',
CLASS_GROUP_02_ID number(38, 0),
CLASS_GROUP_02_CODE number(38, 0),
CLASS_ID number(38, 0),
CLASS_CODE varchar  COMMENT 'Class code',
CLASS_SHORT_CODE varchar,
CLASS_GROUP_01_ID number(38, 0),
CLASS_GROUP_01_CODE number(38, 0),
SUBDEPARTMENT_ID number(38, 0),
SUBDEPARTMENT_SHORT_CODE varchar,
SUBDEPARTMENT_TYPE_ID number(38, 0),
SUBDEPARTMENT_TYPE_CODE varchar  COMMENT 'Subdepartment type code',
MERCHANDISE_DEPARTMENT_ID number(38, 0),
MERCHANDISE_DEPARTMENT_CODE varchar  COMMENT 'Merchandise department code',
MERCHANDISE_DEPT_GROUP_ID number(38, 0),
MERCHANDISE_DEPT_GROUP_CODE varchar,
MACRO_DEPARTMENT_GROUP_ID number(38, 0),
MACRO_DEPARTMENT_GROUP_CODE varchar  COMMENT 'Macro department group code',
CATEGORY_GROUP_ID number(38, 0),
CATEGORY_GROUP_CODE varchar,
DEPARTMENT_ID number(38, 0),
DEPARTMENT_CODE varchar  COMMENT 'Department code',
DEPARTMENT_GROUP_ID number(38, 0),
DEPARTMENT_GROUP_CODE number(38, 0)  COMMENT 'Department group code',
LINE_ID number(38, 0),
LINE_CODE varchar,
LINE_GROUP_ID number(38, 0),
LINE_GROUP_CODE varchar,
FUNCTION_ID number(38, 0),
FUNCTION_CODE varchar,
TENTATIVE_MD_ID number(38, 0),
TENTATIVE_MD_CODE varchar,
COLOR_GROUP_ID number(38, 0),
COLOR_GROUP_CODE varchar,
SOLD_FLAG varchar,
PREV_STATUS_ID number(38, 0),
PREV_STATUS_CODE varchar,
PREV_STATUS_SEASON_YEAR_ID number(38, 0),
PREV_STATUS_SEASON_YEAR_CODE varchar,
PREV_STATUS_EVENT_ID number(38, 0),
PREV_STATUS_EVENT_CODE varchar,
CURR_STATUS_ID number(38, 0),
CURR_STATUS_CODE varchar,
CURR_STATUS_SEASON_YEAR_ID number(38, 0),
CURR_STATUS_SEASON_YEAR_CODE varchar,
CURR_STATUS_EVENT_ID number(38, 0),
CURR_STATUS_EVENT_CODE varchar,
NEXT_STATUS_ID number(38, 0),
NEXT_STATUS_CODE varchar,
NEXT_STATUS_SEASON_YEAR_ID number(38, 0),
NEXT_STATUS_SEASON_YEAR_CODE varchar,
NEXT_STATUS_EVENT_ID number(38, 0),
NEXT_STATUS_EVENT_CODE varchar,
OPERATION_FLAG varchar  COMMENT 'Last operation : I / D / U = INSERT / DELETE / UPDATE',
INSERT_DATE DATETIME  COMMENT 'First insertion date  (yyyy-mm-dd hh-mm-ss)',
UPDATE_DATE DATETIME  COMMENT 'Last update date (yyyy-mm-dd hh-mm-ss)',
__DELETE_FLAG boolean  COMMENT 'Logical deletion flag: true = deleted, false = valid',
__DUMMY_FLAG BOOLEAN,
__DQ_FLAG BOOLEAN,
DATE_PARTITION varchar  COMMENT 'Full copy timestamp  (YYYY-MM-DD HH24:MI:SS)',
ldts timestamp,
source_file varchar
);

create or replace table  stg_sales_raw (
    PARTITION_KEY NUMBER(38,0)  COMMENT 'Original Partition key',
    SUBPARTITION_KEY STRING  COMMENT 'Audit key. A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
    TICKET_CODE STRING  COMMENT 'Ticket code',
    TICKET_LINE_CODE STRING  COMMENT 'Ticket line',
    LOCATION_ID number(38,0)  COMMENT 'Location Key',
    LOCATION_ORIGINAL_ID number(38,0)  COMMENT 'Original Location Key',
    BUSINESS_DATE DATE  COMMENT 'Business Date (yyyy-mm-dd)',
    REGISTER_CODE number(38,0)  COMMENT 'Register Key (Cashier machine number)',
    TRANSACTION_TIME DATETIME  COMMENT 'Transaction Date (yyyy-mm-dd hh-mm-ss)',
    TRANSACTION_TYPE_ID number(38,0)  COMMENT 'Transaction Type ID',
    TRANSACTION_TYPE_CODE STRING  COMMENT 'Transaction Type Key',
    SKU_ID number(38,0)  COMMENT 'Item ID',
    SALE_ASSISTANT_ID number(38,0)  COMMENT 'Sale Assistant ID for Audited Sales Only',
    SALE_ASSISTANT_CODE STRING  COMMENT 'Sale Assistant Key for Audited Sales Only',
    CUSTOMER_CODE STRING  COMMENT 'Customer Key for Audited Sales Only',
    QTY number(38,0)  COMMENT 'Item(s) Quantity',
    AMT_DISCOUNTED number(38,0)  COMMENT 'Amount Payed for the Ticket Line, VAT excluded',
    AMT_DISCOUNTED_TAXED number(38,0)  COMMENT 'Amount Payed for the Ticket Line, VAT included',
    AMT_TAX number(38,0)  COMMENT 'VAT',
    AMT_DISCOUNT number(38,0)  COMMENT 'Total Discount Applied to the Ticket Line, VAT excluded',
    AMT_DISCOUNT_TAXED number(38,0)  COMMENT 'Total Discount Applied to the Ticket Line, VAT included',
    CURRENCY_ID number(38,0)  COMMENT 'Ticket Currency Key',
    CLIENTELE_AREA_STATE_ID number(38,0)  COMMENT 'Clientele Area Regional ID',
    CLIENTELE_AREA_REGIONAL_ID number(38,0)  COMMENT 'Clientele Area Regional ID',
    CLIENTELE_AREA_WW_ID number(38,0)  COMMENT 'Clientele Area WW ID',
    MARK_DOWN_ID number(38,0)  COMMENT '1 = Mark Down, 3 = Preview, 2 = Full Price',
    LOCAL_MARK_DOWN_ID number(38,0)  COMMENT '2 = Mark Down, 3 = Preview, 2 = Full Price',
    SALE_MARK_DOWN_ID number(38,0)  COMMENT '3 = Mark Down, 3 = Preview, 2 = Full Price',
    TYPOLOGY_ID number(38,0)  COMMENT 'typology event key',
    TYPOLOGY_CODE STRING  COMMENT 'typology event code',
    TICKET_LINE_TYPE STRING  COMMENT 'SALE / RETURN',
    AUDIT_TYPE_ID number(38,0)  COMMENT '1 = Audited, 3 = Historical, 5 = POS (Unaudited), 6 = Web Flash (Manual)',
    AUDIT_TYPE_CODE STRING  COMMENT 'A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
    TAG_SOURCE_CODE STRING  COMMENT 'useless',
    OPERATION_FLAG STRING  COMMENT 'Last operation : I / D / U = INSERT / DELETE / UPDATE',
    INSERT_DATE DATETIME  COMMENT 'First insertion date  (yyyy-mm-dd hh-mm-ss)',
    UPDATE_DATE DATETIME  COMMENT 'Last update date (yyyy-mm-dd hh-mm-ss)',
    __INSERT_TSTAMP VARCHAR  COMMENT 'Insert timestamp (YYYY-MM-DD HH24:MI:SS)',
    __UPDATE_TSTAMP VARCHAR  COMMENT 'Update timestamp (YYYY-MM-DD HH24:MI:SS)',
    __DELETE_FLAG BOOLEAN  COMMENT 'Logical deletion flag: true = deleted, false = valid',
    DATE_PARTITION VARCHAR  COMMENT 'Full copy timestamp  (YYYY-MM-DD HH24:MI:SS)',
    ldts timestamp,
    source_file varchar,
    TRANSACTION_SALES NUMBER(38,0) COMMENT 'Unique KEY for each row'
);

 -------------------------------------------- Create  Pipes to ingest data from Stages to Tables ---------------------


----- Create Sequence
CREATE OR REPLACE SEQUENCE seq_transaction_01 START = 1 INCREMENT = 1;

CREATE OR REPLACE PIPE STG_SALES_PP as 

copy into stg_sales_raw from (
select  $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 
,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,$21 ,$22 ,$23 ,$24 ,$25 ,$26 ,$27 ,$28 ,$29 ,$30 ,$31 
,$32 ,$33 ,$34 ,$35 ,$36 ,$37 ,$38 ,$39 ,$40 ,$41
, current_timestamp()
, metadata$filename
,  seq_transaction_01.nextval
 from @sales_data) ;
-- ALTER PIPE stg_sales_pp REFRESH ;

 create or replace pipe stg_location_pp   
as
copy into stg_location_raw from (
select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 
,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,$21 ,$22 ,$23 ,$24 ,$25 ,$26 ,$27 ,$28 ,$29 ,$30 ,$31 
,$32 ,$33 ,$34 ,$35 ,$36 ,$37 ,$38 ,$39 ,$40 ,$41 ,$42 ,$43 ,$44 ,$45 ,$46 ,$47 ,$48 ,$49 ,$50 ,$51 ,$52 
,$53 ,$54 ,$55 ,$56 ,$57 ,$58 ,$59 ,$60 ,$61 ,$62 ,$63 ,$64 ,$65 ,$66 ,$67 ,$68 ,$69 ,$70 ,$71 ,$72
,current_timestamp()
,metadata$filename
 from @location_data);

-- ALTER PIPE stg_location_pp REFRESH ;

create or replace pipe stg_product_pp as
copy into stg_product_raw from (
select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 
,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,$21 ,$22 ,$23 ,$24 ,$25 ,$26 ,$27 ,$28 ,$29 ,$30 ,$31 
,$32 ,$33 ,$34 ,$35 ,$36 ,$37 ,$38 ,$39 ,$40 ,$41 ,$42 ,$43 ,$44 ,$45 ,$46 ,$47 ,$48 ,$49 ,$50 ,$51 ,$52 
,$53 ,$54 ,$55 ,$56 ,$57 ,$58 ,$59 ,$60 ,$61 ,$62 ,$63 ,$64 ,$65 ,$66 ,$67 ,$68 ,$69 ,$70 ,$71 ,$72 ,$73 
,$74 ,$75 ,$76 ,$77 ,$78 ,$79 ,$80 ,$81 ,$82 ,$83 ,$84 ,$85 ,$86 ,$87 ,$88 ,$89 ,$90 ,$91 ,$92 ,$93 ,$94 ,$95 ,$96
,current_timestamp()
,metadata$filename
 from @product_data) ;

-- ALTER PIPE stg_product_pp REFRESH ;

 -------------------------------------------- Create CLEAN Staging Tables structure ---------------------

create or replace TABLE STAGING.STG_SALES (
	PARTITION_KEY NUMBER(38,0) COMMENT 'Original Partition key',
	SUBPARTITION_KEY VARCHAR(16777216) COMMENT 'Audit key. A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
	TICKET_CODE VARCHAR(16777216) COMMENT 'Ticket code',
	TICKET_LINE_CODE VARCHAR(16777216) COMMENT 'Ticket line',
	LOCATION_ID NUMBER(38,0) COMMENT 'Location Key',
	LOCATION_ORIGINAL_ID NUMBER(38,0) COMMENT 'Original Location Key',
	BUSINESS_DATE DATE COMMENT 'Business Date (yyyy-mm-dd)',
	REGISTER_CODE NUMBER(38,0) COMMENT 'Register Key (Cashier machine number)',
	TRANSACTION_TIME TIMESTAMP_NTZ(9) COMMENT 'Transaction Date (yyyy-mm-dd hh-mm-ss)',
	TRANSACTION_TYPE_ID NUMBER(38,0) COMMENT 'Transaction Type ID',
	TRANSACTION_TYPE_CODE VARCHAR(16777216) COMMENT 'Transaction Type Key',
	SKU_ID NUMBER(38,0) COMMENT 'Item ID',
	SALE_ASSISTANT_ID NUMBER(38,0) COMMENT 'Sale Assistant ID for Audited Sales Only',
	SALE_ASSISTANT_CODE VARCHAR(16777216) COMMENT 'Sale Assistant Key for Audited Sales Only',
	CUSTOMER_CODE VARCHAR(16777216) COMMENT 'Customer Key for Audited Sales Only',
	QTY NUMBER(38,0) COMMENT 'Item(s) Quantity',
	AMT_DISCOUNTED NUMBER(38,0) COMMENT 'Amount Payed for the Ticket Line, VAT excluded',
	AMT_DISCOUNTED_TAXED NUMBER(38,0) COMMENT 'Amount Payed for the Ticket Line, VAT included',
	AMT_TAX NUMBER(38,0) COMMENT 'VAT',
	AMT_DISCOUNT NUMBER(38,0) COMMENT 'Total Discount Applied to the Ticket Line, VAT excluded',
	AMT_DISCOUNT_TAXED NUMBER(38,0) COMMENT 'Total Discount Applied to the Ticket Line, VAT included',
	CURRENCY_ID NUMBER(38,0) COMMENT 'Ticket Currency Key',
	CLIENTELE_AREA_STATE_ID NUMBER(38,0) COMMENT 'Clientele Area Regional ID',
	CLIENTELE_AREA_REGIONAL_ID NUMBER(38,0) COMMENT 'Clientele Area Regional ID',
	CLIENTELE_AREA_WW_ID NUMBER(38,0) COMMENT 'Clientele Area WW ID',
	MARK_DOWN_ID NUMBER(38,0) COMMENT '1 = Mark Down, 3 = Preview, 2 = Full Price',
	LOCAL_MARK_DOWN_ID NUMBER(38,0) COMMENT '2 = Mark Down, 3 = Preview, 2 = Full Price',
	SALE_MARK_DOWN_ID NUMBER(38,0) COMMENT '3 = Mark Down, 3 = Preview, 2 = Full Price',
	TYPOLOGY_ID NUMBER(38,0) COMMENT 'typology event key',
	TYPOLOGY_CODE VARCHAR(16777216) COMMENT 'typology event code',
	TICKET_LINE_TYPE VARCHAR(16777216) COMMENT 'SALE / RETURN',
	AUDIT_TYPE_ID NUMBER(38,0) COMMENT '1 = Audited, 3 = Historical, 5 = POS (Unaudited), 6 = Web Flash (Manual)',
	AUDIT_TYPE_CODE VARCHAR(16777216) COMMENT 'A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
	TAG_SOURCE_CODE VARCHAR(16777216) COMMENT 'useless',
	OPERATION_FLAG VARCHAR(16777216) COMMENT 'Last operation : I / D / U = INSERT / DELETE / UPDATE',
	INSERT_DATE TIMESTAMP_NTZ(9) COMMENT 'First insertion date  (yyyy-mm-dd hh-mm-ss)',
	UPDATE_DATE TIMESTAMP_NTZ(9) COMMENT 'Last update date (yyyy-mm-dd hh-mm-ss)',
	__INSERT_TSTAMP VARCHAR(16777216) COMMENT 'Insert timestamp (YYYY-MM-DD HH24:MI:SS)',
	__UPDATE_TSTAMP VARCHAR(16777216) COMMENT 'Update timestamp (YYYY-MM-DD HH24:MI:SS)',
	__DELETE_FLAG BOOLEAN COMMENT 'Logical deletion flag: true = deleted, false = valid',
	DATE_PARTITION VARCHAR(16777216) COMMENT 'Full copy timestamp  (YYYY-MM-DD HH24:MI:SS)',
	LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216),
	TRANSACTION_SALES NUMBER(38,0) COMMENT 'Unique KEY for each row'
);

create or replace TABLE STAGING.STG_PRODUCT (
    SKU_ID number(38, 0)  COMMENT 'Product + size key',
SIZE_CODE_REF varchar,
SIZE_CODE_DESC varchar,
SELLING_FLAG varchar,
STYLE_COLOR_ID number(38, 0),
STYLE_COLOR_CODE varchar  COMMENT 'Product code (style + material + color)',
STYLE_COLOR_ORIGINAL_CODE varchar,
STYLE_COLOR_RENAMED_CODE varchar,
STYLE_COLOR_BORN_SEA_EVENT_ID number(38, 0),
STYLE_COLOR_BORN_SEA_YEAR_ID number(38, 0),
STYLE_COLOR_BORN_SEA_YEAR_CODE varchar,
STYLE_COLOR_BORN_EVENT_CODE varchar,
STYLE_COLOR_END_SEA_EVENT_ID number(38, 0),
STYLE_COLOR_END_SEA_YEAR_ID number(38, 0),
STYLE_COLOR_END_SEA_YEAR_CODE varchar,
STYLE_COLOR_END_EVENT_CODE varchar,
STYLE_ID number(38, 0),
STYLE_CODE varchar  COMMENT 'Product code (style + material)',
MODEL_ID number(38, 0),
MODEL_CODE varchar  COMMENT 'Model code (style)',
MODEL_BORN_SEASON_ID number(38, 0),
MODEL_BORN_SEASON_YEAR_CODE varchar,
MATERIAL_ID number(38, 0),
MATERIAL_CODE varchar,
MATERIAL_TRIM_TYPE_ID number(38, 0),
MATERIAL_TRIM_TYPE_CODE varchar,
COLOR_ID number(38, 0),
COLOR_CODE varchar  COMMENT 'Color code',
COLOR_SUBFAMILY_ID number(38, 0),
COLOR_SUBFAMILY_CODE varchar,
COLOR_FAMILY_ID number(38, 0),
COLOR_FAMILY_CODE varchar,
SUBCLASS_ID number(38, 0),
SUBCLASS_CODE varchar  COMMENT 'Subclass code',
SUBCLASS_SHORT_CODE varchar,
SUBCLASS_GROUP_ID number(38, 0),
SUBCLASS_GROUP_CODE number(38, 0)  COMMENT 'Subclass group code',
CLASS_GROUP_02_ID number(38, 0),
CLASS_GROUP_02_CODE number(38, 0),
CLASS_ID number(38, 0),
CLASS_CODE varchar  COMMENT 'Class code',
CLASS_SHORT_CODE varchar,
CLASS_GROUP_01_ID number(38, 0),
CLASS_GROUP_01_CODE number(38, 0),
SUBDEPARTMENT_ID number(38, 0),
SUBDEPARTMENT_SHORT_CODE varchar,
SUBDEPARTMENT_TYPE_ID number(38, 0),
SUBDEPARTMENT_TYPE_CODE varchar  COMMENT 'Subdepartment type code',
MERCHANDISE_DEPARTMENT_ID number(38, 0),
MERCHANDISE_DEPARTMENT_CODE varchar  COMMENT 'Merchandise department code',
MERCHANDISE_DEPT_GROUP_ID number(38, 0),
MERCHANDISE_DEPT_GROUP_CODE varchar,
MACRO_DEPARTMENT_GROUP_ID number(38, 0),
MACRO_DEPARTMENT_GROUP_CODE varchar  COMMENT 'Macro department group code',
CATEGORY_GROUP_ID number(38, 0),
CATEGORY_GROUP_CODE varchar,
DEPARTMENT_ID number(38, 0),
DEPARTMENT_CODE varchar  COMMENT 'Department code',
DEPARTMENT_GROUP_ID number(38, 0),
DEPARTMENT_GROUP_CODE number(38, 0)  COMMENT 'Department group code',
LINE_ID number(38, 0),
LINE_CODE varchar,
LINE_GROUP_ID number(38, 0),
LINE_GROUP_CODE varchar,
FUNCTION_ID number(38, 0),
FUNCTION_CODE varchar,
TENTATIVE_MD_ID number(38, 0),
TENTATIVE_MD_CODE varchar,
COLOR_GROUP_ID number(38, 0),
COLOR_GROUP_CODE varchar,
SOLD_FLAG varchar,
PREV_STATUS_ID number(38, 0),
PREV_STATUS_CODE varchar,
PREV_STATUS_SEASON_YEAR_ID number(38, 0),
PREV_STATUS_SEASON_YEAR_CODE varchar,
PREV_STATUS_EVENT_ID number(38, 0),
PREV_STATUS_EVENT_CODE varchar,
CURR_STATUS_ID number(38, 0),
CURR_STATUS_CODE varchar,
CURR_STATUS_SEASON_YEAR_ID number(38, 0),
CURR_STATUS_SEASON_YEAR_CODE varchar,
CURR_STATUS_EVENT_ID number(38, 0),
CURR_STATUS_EVENT_CODE varchar,
NEXT_STATUS_ID number(38, 0),
NEXT_STATUS_CODE varchar,
NEXT_STATUS_SEASON_YEAR_ID number(38, 0),
NEXT_STATUS_SEASON_YEAR_CODE varchar,
NEXT_STATUS_EVENT_ID number(38, 0),
NEXT_STATUS_EVENT_CODE varchar,
OPERATION_FLAG varchar  COMMENT 'Last operation : I / D / U = INSERT / DELETE / UPDATE',
INSERT_DATE DATETIME  COMMENT 'First insertion date  (yyyy-mm-dd hh-mm-ss)',
UPDATE_DATE DATETIME  COMMENT 'Last update date (yyyy-mm-dd hh-mm-ss)',
__DELETE_FLAG boolean  COMMENT 'Logical deletion flag: true = deleted, false = valid',
__DUMMY_FLAG BOOLEAN,
__DQ_FLAG BOOLEAN,
DATE_PARTITION varchar  COMMENT 'Full copy timestamp  (YYYY-MM-DD HH24:MI:SS)',
ldts timestamp,
source_file varchar
);


create or replace TABLE STAGING.STG_LOCATION (
	LOCATION_CODE VARCHAR(16777216),
	LOCATION_ID VARCHAR(16777216),
	LOCATION_PLANNING_CODE VARCHAR(16777216),
	CANCELLED_FLAG VARCHAR(16777216),
	DUMMY_FLAG VARCHAR(16777216),
	STORE_GROUP_CODE VARCHAR(16777216),
	CURRENCY_ID VARCHAR(16777216),
	CONCEPT_CODE VARCHAR(16777216),
	SELL_SQUARE_FEET VARCHAR(16777216),
	SELL_MQ VARCHAR(16777216),
	NOSELL_SQUARE_FEET VARCHAR(16777216),
	NOSELL_MQ VARCHAR(16777216),
	GROSS_STORE_SIZE VARCHAR(16777216),
	SDB_CHANNEL_CODE VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_ID VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_CODE VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_TYPOLOGY VARCHAR(16777216),
	LOCATION_SUBTYPE_ID VARCHAR(16777216),
	LOCATION_SUBTYPE_CODE VARCHAR(16777216),
	LOCATION_SUBTYPE_CODE_TYPOLOGY VARCHAR(16777216),
	LOCATION_TYPE_ID VARCHAR(16777216),
	LOCATION_TYPE_CODE VARCHAR(16777216),
	LOCATION_TYPE_CODE_TYPOLOGY VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_ID VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_CODE VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_TYPOLOGY VARCHAR(16777216),
	CITY_ID NUMBER(38,0),
	CITY_CODE VARCHAR(16777216),
	STATE_ID VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_ID VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	DISTRICT_ID VARCHAR(16777216),
	DISTRICT_CODE VARCHAR(16777216),
	ZONE_ID VARCHAR(16777216),
	ZONE_CODE VARCHAR(16777216),
	REGION_ID VARCHAR(16777216),
	REGION_CODE VARCHAR(16777216),
	AREA_ID NUMBER(38,0),
	AREA_CODE VARCHAR(16777216),
	MACROAREA_ID VARCHAR(16777216),
	MACROAREA_CODE VARCHAR(16777216),
	LOCATION_STATUS_ID VARCHAR(16777216),
	LOCATION_STATUS_CODE VARCHAR(16777216),
	REPORTING_UNIT_ID VARCHAR(16777216),
	REPORTING_UNIT_CODE VARCHAR(16777216),
	MACROZONE_ID VARCHAR(16777216),
	MACROZONE_CODE VARCHAR(16777216),
	CHANNEL_ID NUMBER(38,0),
	CHANNEL_CODE VARCHAR(16777216),
	STORE_GROUP_PLAN_ID VARCHAR(16777216),
	STORE_GROUP_PLAN_CODE VARCHAR(16777216),
	STORE_GROUP_PLAN_TYPOLOGY VARCHAR(16777216),
	CHANNEL_PLAN_ID NUMBER(38,0),
	CHANNEL_PLAN_CODE VARCHAR(16777216),
	CHANNEL_PLAN_TYPOLOGY VARCHAR(16777216),
	COUNTRY_PLAN_ID VARCHAR(16777216),
	COUNTRY_PLAN_CODE VARCHAR(16777216),
	LOCATION_OPENING_DATE VARCHAR(16777216),
	LOCATION_CLOSURE_DATE VARCHAR(16777216),
	CONSOLIDATED_FLAG_ID VARCHAR(16777216),
	CONSOLIDATED_FLAG_CODE VARCHAR(16777216),
	TIER_CITY_ID VARCHAR(16777216),
	TIER_CITY_CODE VARCHAR(16777216),
	CHANNEL_PLAN_SORT NUMBER(38,0),
	OPERATION_FLAG VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_NTZ(9),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	__DELETE_FLAG BOOLEAN,
	__DQ_FLAG BOOLEAN,
	__DUMMY_FLAG BOOLEAN,
	LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);

------------------------------- Create STREAMS to detect changes on RAW TABLES -----------------
create or replace stream STAGING.STG_LOCATION_STRM on table STAGING.STG_LOCATION_RAW append_only = TRUE;
create or replace stream STAGING.STG_PRODUCT_STRM on table STAGING.STG_PRODUCT_RAW append_only = TRUE;
create or replace stream STAGING.STG_SALES_STRM on table STAGING.STG_SALES_RAW append_only = TRUE ;

------------------------------- Create TASKS to insert NEW data to CLEAN Tables from RAW tables -----------------
create or replace task STAGING.STG_LOCATION_TSK
	warehouse={{warehouse}}
	when system$stream_has_data('staging.stg_location_strm')
	as insert into stg_location with no_date as (
select * exclude(date_partition, metadata$action,METADATA$ROW_ID
,METADATA$ISUPDATE
 ) from stg_location_strm
)
select distinct * from no_date;
CREATE OR REPLACE TASK STAGING.STG_PRODUCT_TSK
    WAREHOUSE = {{warehouse}}
    WHEN SYSTEM$STREAM_HAS_DATA('staging.stg_product_strm')
AS
INSERT OVERWRITE INTO stg_product
WITH no_metadata AS (
    SELECT * 
    EXCLUDE(metadata$action, metadata$row_id, metadata$isupdate) 
    FROM stg_product_strm
),
current_table AS (
    SELECT * 
    FROM stg_product
),
combined_data AS (
    SELECT * 
    FROM no_metadata
    UNION ALL
    SELECT * 
    FROM current_table
),
deduplicates AS (
    SELECT *
    FROM combined_data
    WHERE sku_id <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY date_partition DESC) = 1
)
SELECT * 
FROM deduplicates;



create or replace task STAGING.STG_SALES_TSK
	warehouse={{warehouse}}
	when system$stream_has_data('staging.stg_sales_strm')
	as insert into stg_sales with TEST as (
select * exclude(metadata$action,METADATA$ROW_ID,METADATA$ISUPDATE)
from stg_sales_strm
)
select * from TEST;

ALTER TASK staging.STG_LOCATION_TSK RESUME;
ALTER TASK staging.STG_PRODUCT_TSK RESUME;
ALTER TASK staging.STG_SALES_TSK RESUME;

------------------------------- Create Streams on RDTV schema (first bcs views will take from STREAMS) -----------------

create or replace stream RAW_DTV.LOCATION_OUTBOUND_STRM on table STAGING.STG_LOCATION append_only = TRUE;
create or replace stream RAW_DTV.PRODUCT_OUTBOUND_STRM on table STAGING.STG_PRODUCT append_only = TRUE;
create or replace stream RAW_DTV.SALES_OUTBOUND_STRM on table STAGING.STG_SALES append_only = TRUE;

-------------------------------- RAW DATA VAULT SCHEMA  ----------------------------------------
use schema raw_dtv;
------------------------------- Create VIEWS to outbound (move from STG TO RDTV) -----------------

create or replace view RAW_DTV.LOCATION_OUTBOUND_VIEW as 
SELECT src.*
--------------------------------------------------------------------
-- derived business key
--------------------------------------------------------------------
     , SHA1_BINARY(UPPER(TRIM(LOCATION_ID)))  sha1_hub_location   
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( 
                                              NVL(TRIM(LOCATION_CODE)       ,'-1')
                                            , NVL(TRIM(LOCATION_PLANNING_CODE)    ,'-1')              
                                            , NVL(TRIM(CURRENCY_ID) ,'-1')                 
                                            , NVL(TRIM(CONCEPT_CODE)      ,'-1')            
                                            , NVL(TRIM(CITY_ID)    ,'-1')               
                                            , NVL(TRIM(STATE_ID) ,'-1')                 
                                            , NVL(TRIM(AREA_ID)    ,'-1')               
                                            ), '^')))  AS LOCATION_hash_diff

  FROM RAW_DTV.LOCATION_OUTBOUND_STRM src
;

create or replace view RAW_DTV.PRODUCT_OUTBOUND_VIEW as 
SELECT src.* exclude (date_partition)
--------------------------------------------------------------------
-- derived business key
--------------------------------------------------------------------
     , SHA1_BINARY(UPPER(TRIM(SKU_ID)))  sha1_hub_product
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( 
                                              NVL(TRIM(NEXT_STATUS_ID)       ,'-1')
                                            , NVL(TRIM(CLASS_ID)    ,'-1')              
                                            , NVL(TRIM(NEXT_STATUS_EVENT_ID) ,'-1')                 
                                            , NVL(TRIM(SUBCLASS_ID)      ,'-1')            
                                            , NVL(TRIM(MODEL_ID)    ,'-1')               
                                            , NVL(TRIM(MATERIAL_ID) ,'-1')                 
                                            , NVL(TRIM(COLOR_SUBFAMILY_ID)    ,'-1')               
                                            ), '^')))  AS product_hash_diff

  FROM RAW_DTV.PRODUCT_OUTBOUND_STRM src
;

create or replace view RAW_DTV.SALES_OUTBOUND_VIEW
as
SELECT src.*
--------------------------------------------------------------------
-- derived business key
--------------------------------------------------------------------
     , SHA1_BINARY(UPPER(TRIM(TRANSACTION_SALES)))  sha1_hub_sales
     , SHA1_BINARY(UPPER(TRIM(LOCATION_ID)))  sha1_hub_location
     , SHA1_BINARY(UPPER(TRIM(SKU_ID)))  sha1_hub_product
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(LOCATION_ID)       ,'-1')
                                                        , NVL(TRIM(TRANSACTION_SALES)        ,'-1')
                                                        ), '^')))  AS sha1_lnk_location_sales
    , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(sku_id)       ,'-1')
                                                        , NVL(TRIM(TRANSACTION_SALES)        ,'-1')
                                                        ), '^')))  AS sha1_lnk_product_sales
    , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( 
                                              NVL(TRIM(TICKET_LINE_CODE)       ,'-1')
                                            , NVL(TRIM(TICKET_CODE)    ,'-1')              
                                            , NVL(TRIM(QTY) ,'-1')                 
                                            , NVL(TRIM(CUSTOMER_CODE)      ,'-1')            
                                            , NVL(TRIM(TYPOLOGY_ID)    ,'-1')               
                                            , NVL(TRIM(SALE_MARK_DOWN_ID) ,'-1')                 
                                            , NVL(TRIM(AUDIT_TYPE_ID)    ,'-1')               
                                            ), '^')))  AS sales_hash_diff


  FROM RAW_DTV.SALES_OUTBOUND_STRM src
;



------------------------HUBs---------------------------------------------------
create or replace table raw_dtv.hub_location (
    sha1_hub_location binary,
    location_id varchar,
    CONSTRAINT pk_hub_location PRIMARY KEY(sha1_hub_location),
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);

create or replace table raw_dtv.hub_product (
    sha1_hub_product binary,
    sku_id varchar,
    CONSTRAINT pk_hub_product PRIMARY KEY(sha1_hub_product),
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);

create or replace table raw_dtv.hub_sales (
    sha1_hub_sales binary,
    transaction_sales varchar,
    CONSTRAINT pk_hub_sales PRIMARY KEY(sha1_hub_sales),
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);

------------------------SATs-----------------------------------------

create or replace table raw_dtv.sat_location (
    sha1_hub_location binary,
    LOCATION_CODE VARCHAR(16777216),
	-- LOCATION_ID VARCHAR(16777216),
	LOCATION_PLANNING_CODE VARCHAR(16777216),
	CANCELLED_FLAG VARCHAR(16777216),
	DUMMY_FLAG VARCHAR(16777216),
	STORE_GROUP_CODE VARCHAR(16777216),
	CURRENCY_ID VARCHAR(16777216),
	CONCEPT_CODE VARCHAR(16777216),
	SELL_SQUARE_FEET VARCHAR(16777216),
	SELL_MQ VARCHAR(16777216),
	NOSELL_SQUARE_FEET VARCHAR(16777216),
	NOSELL_MQ VARCHAR(16777216),
	GROSS_STORE_SIZE VARCHAR(16777216),
	SDB_CHANNEL_CODE VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_ID VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_CODE VARCHAR(16777216),
	LOCATION_SUBSUBTYPE_TYPOLOGY VARCHAR(16777216),
	LOCATION_SUBTYPE_ID VARCHAR(16777216),
	LOCATION_SUBTYPE_CODE VARCHAR(16777216),
	LOCATION_SUBTYPE_CODE_TYPOLOGY VARCHAR(16777216),
	LOCATION_TYPE_ID VARCHAR(16777216),
	LOCATION_TYPE_CODE VARCHAR(16777216),
	LOCATION_TYPE_CODE_TYPOLOGY VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_ID VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_CODE VARCHAR(16777216),
	DISTRIBUTION_CHANNEL_TYPOLOGY VARCHAR(16777216),
	CITY_ID NUMBER(38,0),
	CITY_CODE VARCHAR(16777216),
	STATE_ID VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_ID VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	DISTRICT_ID VARCHAR(16777216),
	DISTRICT_CODE VARCHAR(16777216),
	ZONE_ID VARCHAR(16777216),
	ZONE_CODE VARCHAR(16777216),
	REGION_ID VARCHAR(16777216),
	REGION_CODE VARCHAR(16777216),
	AREA_ID NUMBER(38,0),
	AREA_CODE VARCHAR(16777216),
	MACROAREA_ID VARCHAR(16777216),
	MACROAREA_CODE VARCHAR(16777216),
	LOCATION_STATUS_ID VARCHAR(16777216),
	LOCATION_STATUS_CODE VARCHAR(16777216),
	REPORTING_UNIT_ID VARCHAR(16777216),
	REPORTING_UNIT_CODE VARCHAR(16777216),
	MACROZONE_ID VARCHAR(16777216),
	MACROZONE_CODE VARCHAR(16777216),
	CHANNEL_ID NUMBER(38,0),
	CHANNEL_CODE VARCHAR(16777216),
	STORE_GROUP_PLAN_ID VARCHAR(16777216),
	STORE_GROUP_PLAN_CODE VARCHAR(16777216),
	STORE_GROUP_PLAN_TYPOLOGY VARCHAR(16777216),
	CHANNEL_PLAN_ID NUMBER(38,0),
	CHANNEL_PLAN_CODE VARCHAR(16777216),
	CHANNEL_PLAN_TYPOLOGY VARCHAR(16777216),
	COUNTRY_PLAN_ID VARCHAR(16777216),
	COUNTRY_PLAN_CODE VARCHAR(16777216),
	LOCATION_OPENING_DATE VARCHAR(16777216),
	LOCATION_CLOSURE_DATE VARCHAR(16777216),
	CONSOLIDATED_FLAG_ID VARCHAR(16777216),
	CONSOLIDATED_FLAG_CODE VARCHAR(16777216),
	TIER_CITY_ID VARCHAR(16777216),
	TIER_CITY_CODE VARCHAR(16777216),
	CHANNEL_PLAN_SORT NUMBER(38,0),
	OPERATION_FLAG VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_NTZ(9),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	__DELETE_FLAG BOOLEAN,
	__DQ_FLAG BOOLEAN,
	__DUMMY_FLAG BOOLEAN,
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216),
    HASH_DIFF BINARY
);

create or replace table raw_dtv.sat_product (
    sha1_hub_product binary,
    SIZE_CODE_REF VARCHAR(16777216),
	SIZE_CODE_DESC VARCHAR(16777216),
	SELLING_FLAG VARCHAR(16777216),
	STYLE_COLOR_ID NUMBER(38,0),
	STYLE_COLOR_CODE VARCHAR(16777216),
	STYLE_COLOR_ORIGINAL_CODE VARCHAR(16777216),
	STYLE_COLOR_RENAMED_CODE VARCHAR(16777216),
	STYLE_COLOR_BORN_SEA_EVENT_ID NUMBER(38,0),
	STYLE_COLOR_BORN_SEA_YEAR_ID NUMBER(38,0),
	STYLE_COLOR_BORN_SEA_YEAR_CODE VARCHAR(16777216),
	STYLE_COLOR_BORN_EVENT_CODE VARCHAR(16777216),
	STYLE_COLOR_END_SEA_EVENT_ID NUMBER(38,0),
	STYLE_COLOR_END_SEA_YEAR_ID NUMBER(38,0),
	STYLE_COLOR_END_SEA_YEAR_CODE VARCHAR(16777216),
	STYLE_COLOR_END_EVENT_CODE VARCHAR(16777216),
	STYLE_ID NUMBER(38,0),
	STYLE_CODE VARCHAR(16777216),
	MODEL_ID NUMBER(38,0),
	MODEL_CODE VARCHAR(16777216),
	MODEL_BORN_SEASON_ID NUMBER(38,0),
	MODEL_BORN_SEASON_YEAR_CODE VARCHAR(16777216),
	MATERIAL_ID NUMBER(38,0),
	MATERIAL_CODE VARCHAR(16777216),
	MATERIAL_TRIM_TYPE_ID NUMBER(38,0),
	MATERIAL_TRIM_TYPE_CODE VARCHAR(16777216),
	COLOR_ID NUMBER(38,0),
	COLOR_CODE VARCHAR(16777216),
	COLOR_SUBFAMILY_ID NUMBER(38,0),
	COLOR_SUBFAMILY_CODE VARCHAR(16777216),
	COLOR_FAMILY_ID NUMBER(38,0),
	COLOR_FAMILY_CODE VARCHAR(16777216),
	SUBCLASS_ID NUMBER(38,0),
	SUBCLASS_CODE VARCHAR(16777216),
	SUBCLASS_SHORT_CODE VARCHAR(16777216),
	SUBCLASS_GROUP_ID NUMBER(38,0),
	SUBCLASS_GROUP_CODE NUMBER(38,0),
	CLASS_GROUP_02_ID NUMBER(38,0),
	CLASS_GROUP_02_CODE NUMBER(38,0),
	CLASS_ID NUMBER(38,0),
	CLASS_CODE VARCHAR(16777216),
	CLASS_SHORT_CODE VARCHAR(16777216),
	CLASS_GROUP_01_ID NUMBER(38,0),
	CLASS_GROUP_01_CODE NUMBER(38,0),
	SUBDEPARTMENT_ID NUMBER(38,0),
	SUBDEPARTMENT_SHORT_CODE VARCHAR(16777216),
	SUBDEPARTMENT_TYPE_ID NUMBER(38,0),
	SUBDEPARTMENT_TYPE_CODE VARCHAR(16777216),
	MERCHANDISE_DEPARTMENT_ID NUMBER(38,0),
	MERCHANDISE_DEPARTMENT_CODE VARCHAR(16777216),
	MERCHANDISE_DEPT_GROUP_ID NUMBER(38,0),
	MERCHANDISE_DEPT_GROUP_CODE VARCHAR(16777216),
	MACRO_DEPARTMENT_GROUP_ID NUMBER(38,0),
	MACRO_DEPARTMENT_GROUP_CODE VARCHAR(16777216),
	CATEGORY_GROUP_ID NUMBER(38,0),
	CATEGORY_GROUP_CODE VARCHAR(16777216),
	DEPARTMENT_ID NUMBER(38,0),
	DEPARTMENT_CODE VARCHAR(16777216),
	DEPARTMENT_GROUP_ID NUMBER(38,0),
	DEPARTMENT_GROUP_CODE NUMBER(38,0),
	LINE_ID NUMBER(38,0),
	LINE_CODE VARCHAR(16777216),
	LINE_GROUP_ID NUMBER(38,0),
	LINE_GROUP_CODE VARCHAR(16777216),
	FUNCTION_ID NUMBER(38,0),
	FUNCTION_CODE VARCHAR(16777216),
	TENTATIVE_MD_ID NUMBER(38,0),
	TENTATIVE_MD_CODE VARCHAR(16777216),
	COLOR_GROUP_ID NUMBER(38,0),
	COLOR_GROUP_CODE VARCHAR(16777216),
	SOLD_FLAG VARCHAR(16777216),
	PREV_STATUS_ID NUMBER(38,0),
	PREV_STATUS_CODE VARCHAR(16777216),
	PREV_STATUS_SEASON_YEAR_ID NUMBER(38,0),
	PREV_STATUS_SEASON_YEAR_CODE VARCHAR(16777216),
	PREV_STATUS_EVENT_ID NUMBER(38,0),
	PREV_STATUS_EVENT_CODE VARCHAR(16777216),
	CURR_STATUS_ID NUMBER(38,0),
	CURR_STATUS_CODE VARCHAR(16777216),
	CURR_STATUS_SEASON_YEAR_ID NUMBER(38,0),
	CURR_STATUS_SEASON_YEAR_CODE VARCHAR(16777216),
	CURR_STATUS_EVENT_ID NUMBER(38,0),
	CURR_STATUS_EVENT_CODE VARCHAR(16777216),
	NEXT_STATUS_ID NUMBER(38,0),
	NEXT_STATUS_CODE VARCHAR(16777216),
	NEXT_STATUS_SEASON_YEAR_ID NUMBER(38,0),
	NEXT_STATUS_SEASON_YEAR_CODE VARCHAR(16777216),
	NEXT_STATUS_EVENT_ID NUMBER(38,0),
	NEXT_STATUS_EVENT_CODE VARCHAR(16777216),
	OPERATION_FLAG VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_NTZ(9),
	__DELETE_FLAG BOOLEAN,
	__DUMMY_FLAG BOOLEAN,
	__DQ_FLAG BOOLEAN,
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216),
    HASH_DIFF BINARY
);

create or replace table raw_dtv.sat_sales (
    sha1_hub_sales binary,
	PARTITION_KEY NUMBER(38,0) COMMENT 'Original Partition key',
	SUBPARTITION_KEY VARCHAR(16777216) COMMENT 'Audit key. A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
	TICKET_CODE VARCHAR(16777216) COMMENT 'Ticket code',
	TICKET_LINE_CODE VARCHAR(16777216) COMMENT 'Ticket line',
	LOCATION_ID NUMBER(38,0) COMMENT 'Location Key',
	LOCATION_ORIGINAL_ID NUMBER(38,0) COMMENT 'Original Location Key',
	BUSINESS_DATE DATE COMMENT 'Business Date (yyyy-mm-dd)',
	REGISTER_CODE NUMBER(38,0) COMMENT 'Register Key (Cashier machine number)',
	TRANSACTION_TIME TIMESTAMP_NTZ(9) COMMENT 'Transaction Date (yyyy-mm-dd hh-mm-ss)',
	TRANSACTION_TYPE_ID NUMBER(38,0) COMMENT 'Transaction Type ID',
	TRANSACTION_TYPE_CODE VARCHAR(16777216) COMMENT 'Transaction Type Key',
	SKU_ID NUMBER(38,0) COMMENT 'Item ID',
	SALE_ASSISTANT_ID NUMBER(38,0) COMMENT 'Sale Assistant ID for Audited Sales Only',
	SALE_ASSISTANT_CODE VARCHAR(16777216) COMMENT 'Sale Assistant Key for Audited Sales Only',
	CUSTOMER_CODE VARCHAR(16777216) COMMENT 'Customer Key for Audited Sales Only',
	QTY NUMBER(38,0) COMMENT 'Item(s) Quantity',
	AMT_DISCOUNTED NUMBER(38,0) COMMENT 'Amount Payed for the Ticket Line, VAT excluded',
	AMT_DISCOUNTED_TAXED NUMBER(38,0) COMMENT 'Amount Payed for the Ticket Line, VAT included',
	AMT_TAX NUMBER(38,0) COMMENT 'VAT',
	AMT_DISCOUNT NUMBER(38,0) COMMENT 'Total Discount Applied to the Ticket Line, VAT excluded',
	AMT_DISCOUNT_TAXED NUMBER(38,0) COMMENT 'Total Discount Applied to the Ticket Line, VAT included',
	CURRENCY_ID NUMBER(38,0) COMMENT 'Ticket Currency Key',
	CLIENTELE_AREA_STATE_ID NUMBER(38,0) COMMENT 'Clientele Area Regional ID',
	CLIENTELE_AREA_REGIONAL_ID NUMBER(38,0) COMMENT 'Clientele Area Regional ID',
	CLIENTELE_AREA_WW_ID NUMBER(38,0) COMMENT 'Clientele Area WW ID',
	MARK_DOWN_ID NUMBER(38,0) COMMENT '1 = Mark Down, 3 = Preview, 2 = Full Price',
	LOCAL_MARK_DOWN_ID NUMBER(38,0) COMMENT '2 = Mark Down, 3 = Preview, 2 = Full Price',
	SALE_MARK_DOWN_ID NUMBER(38,0) COMMENT '3 = Mark Down, 3 = Preview, 2 = Full Price',
	TYPOLOGY_ID NUMBER(38,0) COMMENT 'typology event key',
	TYPOLOGY_CODE VARCHAR(16777216) COMMENT 'typology event code',
	TICKET_LINE_TYPE VARCHAR(16777216) COMMENT 'SALE / RETURN',
	AUDIT_TYPE_ID NUMBER(38,0) COMMENT '1 = Audited, 3 = Historical, 5 = POS (Unaudited), 6 = Web Flash (Manual)',
	AUDIT_TYPE_CODE VARCHAR(16777216) COMMENT 'A = Audited, U = Historical, P = POS (Unaudited), W = Web Flash (Manual)',
	TAG_SOURCE_CODE VARCHAR(16777216) COMMENT 'useless',
	OPERATION_FLAG VARCHAR(16777216) COMMENT 'Last operation : I / D / U = INSERT / DELETE / UPDATE',
	INSERT_DATE TIMESTAMP_NTZ(9) COMMENT 'First insertion date  (yyyy-mm-dd hh-mm-ss)',
	UPDATE_DATE TIMESTAMP_NTZ(9) COMMENT 'Last update date (yyyy-mm-dd hh-mm-ss)',
	__INSERT_TSTAMP VARCHAR(16777216) COMMENT 'Insert timestamp (YYYY-MM-DD HH24:MI:SS)',
	__UPDATE_TSTAMP VARCHAR(16777216) COMMENT 'Update timestamp (YYYY-MM-DD HH24:MI:SS)',
	__DELETE_FLAG BOOLEAN COMMENT 'Logical deletion flag: true = deleted, false = valid',
	DATE_PARTITION VARCHAR(16777216) COMMENT 'Full copy timestamp  (YYYY-MM-DD HH24:MI:SS)',
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216),
    HASH_DIFF BINARY
);

-------------------------------------LINKs-------------------------------------------------

create or replace table raw_dtv.lnk_location_sales (
    sha1_lnk_location_sales BINARY     NOT NULL,
    sha1_hub_location BINARY,
    sha1_hub_sales BINARY,
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);

create or replace table raw_dtv.lnk_product_sales (
    sha1_lnk_product_sales BINARY     NOT NULL,
    sha1_hub_product BINARY,
    sha1_hub_sales BINARY,
    LDTS TIMESTAMP_NTZ(9),
	SOURCE_FILE VARCHAR(16777216)
);


---------------------------- Create Tasks to Alimenter Tables of RAW DATAVAULT model & Resume ------

create or replace task RAW_DTV.LOCATION_STRM_TSK
	warehouse={{warehouse}}
	when SYSTEM$STREAM_HAS_DATA('LOCATION_OUTBOUND_STRM')
	as INSERT ALL
WHEN (SELECT COUNT(1) FROM hub_location tgt WHERE tgt.sha1_hub_location = src_sha1_hub_location) = 0
THEN INTO hub_location  
( sha1_hub_location
, location_id
, ldts
, source_file
)  
VALUES 
( src_sha1_hub_location
, src_location_id
, src_ldts
, src_source_file
)  
WHEN (SELECT COUNT(1) FROM sat_location tgt WHERE tgt.sha1_hub_location = src_sha1_hub_location AND tgt.hash_diff = src_hash_diff) = 0
THEN INTO sat_location 
(
    sha1_hub_location,  
    LOCATION_CODE,
    -- LOCATION_ID VARCHAR(16777216),
    LOCATION_PLANNING_CODE,
    CANCELLED_FLAG ,
    DUMMY_FLAG,
    STORE_GROUP_CODE,
    CURRENCY_ID ,
    CONCEPT_CODE,
    SELL_SQUARE_FEET,
    SELL_MQ,
    NOSELL_SQUARE_FEET,
    NOSELL_MQ,
    GROSS_STORE_SIZE,
    SDB_CHANNEL_CODE,
    LOCATION_SUBSUBTYPE_ID,
    LOCATION_SUBSUBTYPE_CODE,
    LOCATION_SUBSUBTYPE_TYPOLOGY,
    LOCATION_SUBTYPE_ID ,
    LOCATION_SUBTYPE_CODE,
    LOCATION_SUBTYPE_CODE_TYPOLOGY,
    LOCATION_TYPE_ID,
    LOCATION_TYPE_CODE,
    LOCATION_TYPE_CODE_TYPOLOGY,
    DISTRIBUTION_CHANNEL_ID,
    DISTRIBUTION_CHANNEL_CODE,
    DISTRIBUTION_CHANNEL_TYPOLOGY,
    CITY_ID,
    CITY_CODE,
    STATE_ID,
    STATE_CODE,
    COUNTRY_ID,
    COUNTRY_CODE,
    DISTRICT_ID,
    DISTRICT_CODE,
    ZONE_ID,
    ZONE_CODE,
    REGION_ID,
    REGION_CODE,
    AREA_ID,
    AREA_CODE,
    MACROAREA_ID,
    MACROAREA_CODE,
    LOCATION_STATUS_ID,
    LOCATION_STATUS_CODE,
    REPORTING_UNIT_ID,
    REPORTING_UNIT_CODE,
    MACROZONE_ID,
    MACROZONE_CODE,
    CHANNEL_ID,
    CHANNEL_CODE,
    STORE_GROUP_PLAN_ID,
    STORE_GROUP_PLAN_CODE,
    STORE_GROUP_PLAN_TYPOLOGY,
    CHANNEL_PLAN_ID,
    CHANNEL_PLAN_CODE,
    CHANNEL_PLAN_TYPOLOGY,
    COUNTRY_PLAN_ID,
    COUNTRY_PLAN_CODE,
    LOCATION_OPENING_DATE,
    LOCATION_CLOSURE_DATE,
    CONSOLIDATED_FLAG_ID,
    CONSOLIDATED_FLAG_CODE,
    TIER_CITY_ID,
    TIER_CITY_CODE,
    CHANNEL_PLAN_SORT,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __DELETE_FLAG,
    __DQ_FLAG,
    __DUMMY_FLAG,
    HASH_DIFF,
    LDTS,
    SOURCE_FILE
)  
VALUES 
(
    src_sha1_hub_location,  
    LOCATION_CODE,
    -- LOCATION_ID VARCHAR(16777216),
    LOCATION_PLANNING_CODE,
    CANCELLED_FLAG ,
    DUMMY_FLAG,
    STORE_GROUP_CODE,
    CURRENCY_ID ,
    CONCEPT_CODE,
    SELL_SQUARE_FEET,
    SELL_MQ,
    NOSELL_SQUARE_FEET,
    NOSELL_MQ,
    GROSS_STORE_SIZE,
    SDB_CHANNEL_CODE,
    LOCATION_SUBSUBTYPE_ID,
    LOCATION_SUBSUBTYPE_CODE,
    LOCATION_SUBSUBTYPE_TYPOLOGY,
    LOCATION_SUBTYPE_ID ,
    LOCATION_SUBTYPE_CODE,
    LOCATION_SUBTYPE_CODE_TYPOLOGY,
    LOCATION_TYPE_ID,
    LOCATION_TYPE_CODE,
    LOCATION_TYPE_CODE_TYPOLOGY,
    DISTRIBUTION_CHANNEL_ID,
    DISTRIBUTION_CHANNEL_CODE,
    DISTRIBUTION_CHANNEL_TYPOLOGY,
    CITY_ID,
    CITY_CODE,
    STATE_ID,
    STATE_CODE,
    COUNTRY_ID,
    COUNTRY_CODE,
    DISTRICT_ID,
    DISTRICT_CODE,
    ZONE_ID,
    ZONE_CODE,
    REGION_ID,
    REGION_CODE,
    AREA_ID,
    AREA_CODE,
    MACROAREA_ID,
    MACROAREA_CODE,
    LOCATION_STATUS_ID,
    LOCATION_STATUS_CODE,
    REPORTING_UNIT_ID,
    REPORTING_UNIT_CODE,
    MACROZONE_ID,
    MACROZONE_CODE,
    CHANNEL_ID,
    CHANNEL_CODE,
    STORE_GROUP_PLAN_ID,
    STORE_GROUP_PLAN_CODE,
    STORE_GROUP_PLAN_TYPOLOGY,
    CHANNEL_PLAN_ID,
    CHANNEL_PLAN_CODE,
    CHANNEL_PLAN_TYPOLOGY,
    COUNTRY_PLAN_ID,
    COUNTRY_PLAN_CODE,
    LOCATION_OPENING_DATE,
    LOCATION_CLOSURE_DATE,
    CONSOLIDATED_FLAG_ID,
    CONSOLIDATED_FLAG_CODE,
    TIER_CITY_ID,
    TIER_CITY_CODE,
    CHANNEL_PLAN_SORT,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __DELETE_FLAG,
    __DQ_FLAG,
    __DUMMY_FLAG,
    SRC_HASH_DIFF,
    SRC_LDTS,
    SRC_SOURCE_FILE
                
)
SELECT
    sha1_hub_location src_sha1_hub_location,  
    LOCATION_CODE,
    LOCATION_ID src_location_id,
    LOCATION_PLANNING_CODE,
    CANCELLED_FLAG ,
    DUMMY_FLAG,
    STORE_GROUP_CODE,
    CURRENCY_ID ,
    CONCEPT_CODE,
    SELL_SQUARE_FEET,
    SELL_MQ,
    NOSELL_SQUARE_FEET,
    NOSELL_MQ,
    GROSS_STORE_SIZE,
    SDB_CHANNEL_CODE,
    LOCATION_SUBSUBTYPE_ID,
    LOCATION_SUBSUBTYPE_CODE,
    LOCATION_SUBSUBTYPE_TYPOLOGY,
    LOCATION_SUBTYPE_ID ,
    LOCATION_SUBTYPE_CODE,
    LOCATION_SUBTYPE_CODE_TYPOLOGY,
    LOCATION_TYPE_ID,
    LOCATION_TYPE_CODE,
    LOCATION_TYPE_CODE_TYPOLOGY,
    DISTRIBUTION_CHANNEL_ID,
    DISTRIBUTION_CHANNEL_CODE,
    DISTRIBUTION_CHANNEL_TYPOLOGY,
    CITY_ID,
    CITY_CODE,
    STATE_ID,
    STATE_CODE,
    COUNTRY_ID,
    COUNTRY_CODE,
    DISTRICT_ID,
    DISTRICT_CODE,
    ZONE_ID,
    ZONE_CODE,
    REGION_ID,
    REGION_CODE,
    AREA_ID,
    AREA_CODE,
    MACROAREA_ID,
    MACROAREA_CODE,
    LOCATION_STATUS_ID,
    LOCATION_STATUS_CODE,
    REPORTING_UNIT_ID,
    REPORTING_UNIT_CODE,
    MACROZONE_ID,
    MACROZONE_CODE,
    CHANNEL_ID,
    CHANNEL_CODE,
    STORE_GROUP_PLAN_ID,
    STORE_GROUP_PLAN_CODE,
    STORE_GROUP_PLAN_TYPOLOGY,
    CHANNEL_PLAN_ID,
    CHANNEL_PLAN_CODE,
    CHANNEL_PLAN_TYPOLOGY,
    COUNTRY_PLAN_ID,
    COUNTRY_PLAN_CODE,
    LOCATION_OPENING_DATE,
    LOCATION_CLOSURE_DATE,
    CONSOLIDATED_FLAG_ID,
    CONSOLIDATED_FLAG_CODE,
    TIER_CITY_ID,
    TIER_CITY_CODE,
    CHANNEL_PLAN_SORT,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __DELETE_FLAG,
    __DQ_FLAG,
    __DUMMY_FLAG,
    location_HASH_DIFF SRC_HASH_DIFF,
    LDTS SRC_LDTS,
    SOURCE_FILE SRC_SOURCE_FILE
  FROM raw_dtv.LOCATION_OUTBOUND_VIEW src;

  create or replace task RAW_DTV.PRODUCT_STRM_TSK
	warehouse={{warehouse}}
	when SYSTEM$STREAM_HAS_DATA('PRODUCT_OUTBOUND_STRM')
	as INSERT ALL
WHEN (SELECT COUNT(1) FROM hub_product tgt WHERE tgt.sha1_hub_product = src_sha1_hub_product ) = 0
THEN INTO hub_product  
( sha1_hub_product
, sku_id
, LDTS 
, SOURCE_FILE 
)  
VALUES 
( src_sha1_hub_product
, src_sku_id,
     SRC_LDTS,
     SRC_SOURCE_FILE

)  
WHEN (SELECT COUNT(1) FROM sat_product tgt WHERE tgt.sha1_hub_product = src_sha1_hub_product AND tgt.hash_diff = src_hash_diff) = 0
THEN INTO sat_product 
(
    sha1_hub_product,  
    -- SKU_ID ,
    SIZE_CODE_REF ,
    SIZE_CODE_DESC ,
    SELLING_FLAG ,
    STYLE_COLOR_ID ,
    STYLE_COLOR_CODE ,
    STYLE_COLOR_ORIGINAL_CODE ,
    STYLE_COLOR_RENAMED_CODE ,
    STYLE_COLOR_BORN_SEA_EVENT_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_CODE ,
    STYLE_COLOR_BORN_EVENT_CODE ,
    STYLE_COLOR_END_SEA_EVENT_ID ,
    STYLE_COLOR_END_SEA_YEAR_ID ,
    STYLE_COLOR_END_SEA_YEAR_CODE ,
    STYLE_COLOR_END_EVENT_CODE ,
    STYLE_ID ,
    STYLE_CODE ,
    MODEL_ID ,
    MODEL_CODE ,
    MODEL_BORN_SEASON_ID ,
    MODEL_BORN_SEASON_YEAR_CODE ,
    MATERIAL_ID ,
    MATERIAL_CODE ,
    MATERIAL_TRIM_TYPE_ID ,
    MATERIAL_TRIM_TYPE_CODE ,
    COLOR_ID ,
    COLOR_CODE ,
    COLOR_SUBFAMILY_ID ,
    COLOR_SUBFAMILY_CODE ,
    COLOR_FAMILY_ID ,
    COLOR_FAMILY_CODE ,
    SUBCLASS_ID ,
    SUBCLASS_CODE ,
    SUBCLASS_SHORT_CODE ,
    SUBCLASS_GROUP_ID ,
    SUBCLASS_GROUP_CODE ,
    CLASS_GROUP_02_ID ,
    CLASS_GROUP_02_CODE ,
    CLASS_ID ,
    CLASS_CODE ,
    CLASS_SHORT_CODE ,
    CLASS_GROUP_01_ID ,
    CLASS_GROUP_01_CODE ,
    SUBDEPARTMENT_ID ,
    SUBDEPARTMENT_SHORT_CODE ,
    SUBDEPARTMENT_TYPE_ID ,
    SUBDEPARTMENT_TYPE_CODE ,
    MERCHANDISE_DEPARTMENT_ID ,
    MERCHANDISE_DEPARTMENT_CODE ,
    MERCHANDISE_DEPT_GROUP_ID ,
    MERCHANDISE_DEPT_GROUP_CODE ,
    MACRO_DEPARTMENT_GROUP_ID ,
    MACRO_DEPARTMENT_GROUP_CODE ,
    CATEGORY_GROUP_ID ,
    CATEGORY_GROUP_CODE ,
    DEPARTMENT_ID ,
    DEPARTMENT_CODE ,
    DEPARTMENT_GROUP_ID ,
    DEPARTMENT_GROUP_CODE ,
    LINE_ID ,
    LINE_CODE ,
    LINE_GROUP_ID ,
    LINE_GROUP_CODE ,
    FUNCTION_ID ,
    FUNCTION_CODE ,
    TENTATIVE_MD_ID ,
    TENTATIVE_MD_CODE ,
    COLOR_GROUP_ID ,
    COLOR_GROUP_CODE ,
    SOLD_FLAG ,
    PREV_STATUS_ID ,
    PREV_STATUS_CODE ,
    PREV_STATUS_SEASON_YEAR_ID ,
    PREV_STATUS_SEASON_YEAR_CODE ,
    PREV_STATUS_EVENT_ID ,
    PREV_STATUS_EVENT_CODE ,
    CURR_STATUS_ID ,
    CURR_STATUS_CODE ,
    CURR_STATUS_SEASON_YEAR_ID ,
    CURR_STATUS_SEASON_YEAR_CODE ,
    CURR_STATUS_EVENT_ID ,
    CURR_STATUS_EVENT_CODE ,
    NEXT_STATUS_ID ,
    NEXT_STATUS_CODE ,
    NEXT_STATUS_SEASON_YEAR_ID ,
    NEXT_STATUS_SEASON_YEAR_CODE ,
    NEXT_STATUS_EVENT_ID ,
    NEXT_STATUS_EVENT_CODE ,
    OPERATION_FLAG ,
    INSERT_DATE ,
    -- UPDATE_DATE ,
    __DELETE_FLAG ,
    __DUMMY_FLAG ,
    __DQ_FLAG  ,  
    HASH_DIFF ,
    LDTS ,
    SOURCE_FILE 
)  
VALUES 
(
    src_sha1_hub_product,  
    SIZE_CODE_REF ,
    SIZE_CODE_DESC ,
    SELLING_FLAG ,
    STYLE_COLOR_ID ,
    STYLE_COLOR_CODE ,
    STYLE_COLOR_ORIGINAL_CODE ,
    STYLE_COLOR_RENAMED_CODE ,
    STYLE_COLOR_BORN_SEA_EVENT_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_CODE ,
    STYLE_COLOR_BORN_EVENT_CODE ,
    STYLE_COLOR_END_SEA_EVENT_ID ,
    STYLE_COLOR_END_SEA_YEAR_ID ,
    STYLE_COLOR_END_SEA_YEAR_CODE ,
    STYLE_COLOR_END_EVENT_CODE ,
    STYLE_ID ,
    STYLE_CODE ,
    MODEL_ID ,
    MODEL_CODE ,
    MODEL_BORN_SEASON_ID ,
    MODEL_BORN_SEASON_YEAR_CODE ,
    MATERIAL_ID ,
    MATERIAL_CODE ,
    MATERIAL_TRIM_TYPE_ID ,
    MATERIAL_TRIM_TYPE_CODE ,
    COLOR_ID ,
    COLOR_CODE ,
    COLOR_SUBFAMILY_ID ,
    COLOR_SUBFAMILY_CODE ,
    COLOR_FAMILY_ID ,
    COLOR_FAMILY_CODE ,
    SUBCLASS_ID ,
    SUBCLASS_CODE ,
    SUBCLASS_SHORT_CODE ,
    SUBCLASS_GROUP_ID ,
    SUBCLASS_GROUP_CODE ,
    CLASS_GROUP_02_ID ,
    CLASS_GROUP_02_CODE ,
    CLASS_ID ,
    CLASS_CODE ,
    CLASS_SHORT_CODE ,
    CLASS_GROUP_01_ID ,
    CLASS_GROUP_01_CODE ,
    SUBDEPARTMENT_ID ,
    SUBDEPARTMENT_SHORT_CODE ,
    SUBDEPARTMENT_TYPE_ID ,
    SUBDEPARTMENT_TYPE_CODE ,
    MERCHANDISE_DEPARTMENT_ID ,
    MERCHANDISE_DEPARTMENT_CODE ,
    MERCHANDISE_DEPT_GROUP_ID ,
    MERCHANDISE_DEPT_GROUP_CODE ,
    MACRO_DEPARTMENT_GROUP_ID ,
    MACRO_DEPARTMENT_GROUP_CODE ,
    CATEGORY_GROUP_ID ,
    CATEGORY_GROUP_CODE ,
    DEPARTMENT_ID ,
    DEPARTMENT_CODE ,
    DEPARTMENT_GROUP_ID ,
    DEPARTMENT_GROUP_CODE ,
    LINE_ID ,
    LINE_CODE ,
    LINE_GROUP_ID ,
    LINE_GROUP_CODE ,
    FUNCTION_ID ,
    FUNCTION_CODE ,
    TENTATIVE_MD_ID ,
    TENTATIVE_MD_CODE ,
    COLOR_GROUP_ID ,
    COLOR_GROUP_CODE ,
    SOLD_FLAG ,
    PREV_STATUS_ID ,
    PREV_STATUS_CODE ,
    PREV_STATUS_SEASON_YEAR_ID ,
    PREV_STATUS_SEASON_YEAR_CODE ,
    PREV_STATUS_EVENT_ID ,
    PREV_STATUS_EVENT_CODE ,
    CURR_STATUS_ID ,
    CURR_STATUS_CODE ,
    CURR_STATUS_SEASON_YEAR_ID ,
    CURR_STATUS_SEASON_YEAR_CODE ,
    CURR_STATUS_EVENT_ID ,
    CURR_STATUS_EVENT_CODE ,
    NEXT_STATUS_ID ,
    NEXT_STATUS_CODE ,
    NEXT_STATUS_SEASON_YEAR_ID ,
    NEXT_STATUS_SEASON_YEAR_CODE ,
    NEXT_STATUS_EVENT_ID ,
    NEXT_STATUS_EVENT_CODE ,
    OPERATION_FLAG ,
    INSERT_DATE ,
    -- UPDATE_DATE ,
    __DELETE_FLAG ,
    __DUMMY_FLAG ,
    __DQ_FLAG,
                SRC_HASH_DIFF,
    SRC_LDTS,
     SRC_SOURCE_FILE
)
SELECT
    sha1_hub_product src_sha1_hub_product,  
    SKU_ID src_SKU_ID ,
    SIZE_CODE_REF ,
    SIZE_CODE_DESC ,
    SELLING_FLAG ,
    STYLE_COLOR_ID ,
    STYLE_COLOR_CODE ,
    STYLE_COLOR_ORIGINAL_CODE ,
    STYLE_COLOR_RENAMED_CODE ,
    STYLE_COLOR_BORN_SEA_EVENT_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_ID ,
    STYLE_COLOR_BORN_SEA_YEAR_CODE ,
    STYLE_COLOR_BORN_EVENT_CODE ,
    STYLE_COLOR_END_SEA_EVENT_ID ,
    STYLE_COLOR_END_SEA_YEAR_ID ,
    STYLE_COLOR_END_SEA_YEAR_CODE ,
    STYLE_COLOR_END_EVENT_CODE ,
    STYLE_ID ,
    STYLE_CODE ,
    MODEL_ID ,
    MODEL_CODE ,
    MODEL_BORN_SEASON_ID ,
    MODEL_BORN_SEASON_YEAR_CODE ,
    MATERIAL_ID ,
    MATERIAL_CODE ,
    MATERIAL_TRIM_TYPE_ID ,
    MATERIAL_TRIM_TYPE_CODE ,
    COLOR_ID ,
    COLOR_CODE ,
    COLOR_SUBFAMILY_ID ,
    COLOR_SUBFAMILY_CODE ,
    COLOR_FAMILY_ID ,
    COLOR_FAMILY_CODE ,
    SUBCLASS_ID ,
    SUBCLASS_CODE ,
    SUBCLASS_SHORT_CODE ,
    SUBCLASS_GROUP_ID ,
    SUBCLASS_GROUP_CODE ,
    CLASS_GROUP_02_ID ,
    CLASS_GROUP_02_CODE ,
    CLASS_ID ,
    CLASS_CODE ,
    CLASS_SHORT_CODE ,
    CLASS_GROUP_01_ID ,
    CLASS_GROUP_01_CODE ,
    SUBDEPARTMENT_ID ,
    SUBDEPARTMENT_SHORT_CODE ,
    SUBDEPARTMENT_TYPE_ID ,
    SUBDEPARTMENT_TYPE_CODE ,
    MERCHANDISE_DEPARTMENT_ID ,
    MERCHANDISE_DEPARTMENT_CODE ,
    MERCHANDISE_DEPT_GROUP_ID ,
    MERCHANDISE_DEPT_GROUP_CODE ,
    MACRO_DEPARTMENT_GROUP_ID ,
    MACRO_DEPARTMENT_GROUP_CODE ,
    CATEGORY_GROUP_ID ,
    CATEGORY_GROUP_CODE ,
    DEPARTMENT_ID ,
    DEPARTMENT_CODE ,
    DEPARTMENT_GROUP_ID ,
    DEPARTMENT_GROUP_CODE ,
    LINE_ID ,
    LINE_CODE ,
    LINE_GROUP_ID ,
    LINE_GROUP_CODE ,
    FUNCTION_ID ,
    FUNCTION_CODE ,
    TENTATIVE_MD_ID ,
    TENTATIVE_MD_CODE ,
    COLOR_GROUP_ID ,
    COLOR_GROUP_CODE ,
    SOLD_FLAG ,
    PREV_STATUS_ID ,
    PREV_STATUS_CODE ,
    PREV_STATUS_SEASON_YEAR_ID ,
    PREV_STATUS_SEASON_YEAR_CODE ,
    PREV_STATUS_EVENT_ID ,
    PREV_STATUS_EVENT_CODE ,
    CURR_STATUS_ID ,
    CURR_STATUS_CODE ,
    CURR_STATUS_SEASON_YEAR_ID ,
    CURR_STATUS_SEASON_YEAR_CODE ,
    CURR_STATUS_EVENT_ID ,
    CURR_STATUS_EVENT_CODE ,
    NEXT_STATUS_ID ,
    NEXT_STATUS_CODE ,
    NEXT_STATUS_SEASON_YEAR_ID ,
    NEXT_STATUS_SEASON_YEAR_CODE ,
    NEXT_STATUS_EVENT_ID ,
    NEXT_STATUS_EVENT_CODE ,
    OPERATION_FLAG ,
    INSERT_DATE ,
    __DELETE_FLAG ,
    __DUMMY_FLAG ,
    __DQ_FLAG,
    product_HASH_DIFF SRC_HASH_DIFF,
    LDTS SRC_LDTS,
    SOURCE_FILE SRC_SOURCE_FILE
  FROM raw_dtv.PRODUCT_OUTBOUND_VIEW src;

  create or replace task RAW_DTV.SALES_STRM_TSK
	warehouse={{warehouse}}
	when SYSTEM$STREAM_HAS_DATA('SALES_OUTBOUND_STRM')
	as INSERT ALL
WHEN (SELECT COUNT(1) FROM hub_sales tgt WHERE tgt.sha1_hub_sales = src_sha1_hub_sales ) = 0
THEN INTO hub_sales  
( sha1_hub_sales
, transaction_sales
, LDTS ,
SOURCE_FILE 
)  
VALUES 
( src_sha1_hub_sales
, src_transaction_sales,
src_ldts,
 src_source_file
)  
WHEN (SELECT COUNT(1) FROM sat_sales tgt WHERE tgt.sha1_hub_sales = src_sha1_hub_sales AND tgt.hash_diff = src_hash_diff) = 0
THEN INTO sat_sales
(
LDTS ,
SOURCE_FILE ,
HASH_DIFF ,
    SHA1_HUB_SALES,
    PARTITION_KEY,
SUBPARTITION_KEY,
TICKET_CODE,
TICKET_LINE_CODE,
LOCATION_ID,
LOCATION_ORIGINAL_ID,
BUSINESS_DATE,
REGISTER_CODE,
TRANSACTION_TIME,
TRANSACTION_TYPE_ID,
TRANSACTION_TYPE_CODE,
SKU_ID,
SALE_ASSISTANT_ID,
SALE_ASSISTANT_CODE,
CUSTOMER_CODE,
QTY,
AMT_DISCOUNTED,
AMT_DISCOUNTED_TAXED,
AMT_TAX,
AMT_DISCOUNT,
AMT_DISCOUNT_TAXED,
CURRENCY_ID,
CLIENTELE_AREA_STATE_ID,
CLIENTELE_AREA_REGIONAL_ID,
CLIENTELE_AREA_WW_ID,
MARK_DOWN_ID,
LOCAL_MARK_DOWN_ID,
SALE_MARK_DOWN_ID,
TYPOLOGY_ID,
TYPOLOGY_CODE,
TICKET_LINE_TYPE,
AUDIT_TYPE_ID,
AUDIT_TYPE_CODE,
TAG_SOURCE_CODE,
OPERATION_FLAG,
INSERT_DATE,
UPDATE_DATE,
__INSERT_TSTAMP,
__UPDATE_TSTAMP,
__DELETE_FLAG,
DATE_PARTITION           
)  
VALUES 
(
 src_ldts,
 src_source_file,
 src_hash_diff,
    SRC_SHA1_HUB_SALES,
    PARTITION_KEY,
SUBPARTITION_KEY,
TICKET_CODE,
TICKET_LINE_CODE,
LOCATION_ID,
LOCATION_ORIGINAL_ID,
BUSINESS_DATE,
REGISTER_CODE,
TRANSACTION_TIME,
TRANSACTION_TYPE_ID,
TRANSACTION_TYPE_CODE,
SKU_ID,
SALE_ASSISTANT_ID,
SALE_ASSISTANT_CODE,
CUSTOMER_CODE,
QTY,
AMT_DISCOUNTED,
AMT_DISCOUNTED_TAXED,
AMT_TAX,
AMT_DISCOUNT,
AMT_DISCOUNT_TAXED,
CURRENCY_ID,
CLIENTELE_AREA_STATE_ID,
CLIENTELE_AREA_REGIONAL_ID,
CLIENTELE_AREA_WW_ID,
MARK_DOWN_ID,
LOCAL_MARK_DOWN_ID,
SALE_MARK_DOWN_ID,
TYPOLOGY_ID,
TYPOLOGY_CODE,
TICKET_LINE_TYPE,
AUDIT_TYPE_ID,
AUDIT_TYPE_CODE,
TAG_SOURCE_CODE,
OPERATION_FLAG,
INSERT_DATE,
UPDATE_DATE,
__INSERT_TSTAMP,
__UPDATE_TSTAMP,
__DELETE_FLAG,
DATE_PARTITION
                
)
WHEN (SELECT COUNT(1) FROM lnk_product_sales tgt WHERE tgt.sha1_lnk_product_sales = src_sha1_lnk_product_sales ) = 0
THEN INTO   lnk_product_sales
( sha1_lnk_product_sales
, SHA1_HUB_PRODUCT
, SHA1_HUB_SALES
,LDTS ,
SOURCE_FILE 
)  
VALUES 
( src_sha1_lnk_product_sales
, src_SHA1_HUB_PRODUCT
, src_SHA1_HUB_SALES
, src_ldts,
src_source_file
)  

WHEN (SELECT COUNT(1) FROM lnk_location_sales tgt WHERE tgt.sha1_lnk_location_sales = src_sha1_lnk_location_sales ) = 0
THEN INTO lnk_location_sales  
( sha1_lnk_location_sales
, SHA1_HUB_LOCATION
, SHA1_HUB_SALES,
LDTS ,
SOURCE_FILE 
)  
VALUES 
( src_sha1_lnk_location_sales
, src_SHA1_HUB_LOCATION
, src_SHA1_HUB_SALES
, src_ldts,
src_source_file
)  


SELECT
    SHA1_HUB_SALES src_sha1_hub_sales,
    sha1_hub_location src_sha1_hub_location,
        sha1_hub_location src_sha1_hub_product,

    TRANSACTION_SALES src_transaction_sales,
    sha1_lnk_location_sales src_sha1_lnk_location_sales,
        sha1_lnk_product_sales src_sha1_lnk_product_sales,

    PARTITION_KEY,
SUBPARTITION_KEY,
TICKET_CODE,
TICKET_LINE_CODE,
LOCATION_ID,
LOCATION_ORIGINAL_ID,
BUSINESS_DATE,
REGISTER_CODE,
TRANSACTION_TIME,
TRANSACTION_TYPE_ID,
TRANSACTION_TYPE_CODE,
SKU_ID,
SALE_ASSISTANT_ID,
SALE_ASSISTANT_CODE,
CUSTOMER_CODE,
QTY,
AMT_DISCOUNTED,
AMT_DISCOUNTED_TAXED,
AMT_TAX,
AMT_DISCOUNT,
AMT_DISCOUNT_TAXED,
CURRENCY_ID,
CLIENTELE_AREA_STATE_ID,
CLIENTELE_AREA_REGIONAL_ID,
CLIENTELE_AREA_WW_ID,
MARK_DOWN_ID,
LOCAL_MARK_DOWN_ID,
SALE_MARK_DOWN_ID,
TYPOLOGY_ID,
TYPOLOGY_CODE,
TICKET_LINE_TYPE,
AUDIT_TYPE_ID,
AUDIT_TYPE_CODE,
TAG_SOURCE_CODE,
OPERATION_FLAG,
INSERT_DATE,
UPDATE_DATE,
__INSERT_TSTAMP,
__UPDATE_TSTAMP,
__DELETE_FLAG,
DATE_PARTITION,
LDTS src_ldts,
SOURCE_FILE src_source_file,
SALES_HASH_DIFF src_hash_diff
    
  FROM raw_dtv.SALES_OUTBOUND_VIEW src;

ALTER TASK raw_dtv.SALES_STRM_TSK RESUME;
ALTER TASK raw_dtv.PRODUCT_STRM_TSK RESUME;
ALTER TASK raw_dtv.LOCATION_STRM_TSK RESUME;