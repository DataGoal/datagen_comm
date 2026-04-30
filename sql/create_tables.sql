-- =============================================================================
-- DDL SCRIPTS  –  dev_pbi_perform_cf_poc schema
-- =============================================================================
-- These scripts create the Delta Lake tables in Unity Catalog.
-- The PySpark generator writes data after these structures exist.
-- Run this notebook once before the data generator, or let the generator
-- create tables via saveAsTable (which auto-creates them).
-- =============================================================================

-- Prerequisites
CREATE CATALOG IF NOT EXISTS development;
USE CATALOG development;
CREATE SCHEMA IF NOT EXISTS dev_pbi_perform_cf_poc;
USE SCHEMA dev_pbi_perform_cf_poc;

-- ---------------------------------------------------------------------------
-- LOOKUP / REFERENCE TABLES
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE accounting_document_type (
    accounting_document_type_id    BIGINT         NOT NULL,
    accounting_document_type_cd    STRING,
    accounting_document_type_nm    STRING,
    active_ind                     STRING,
    language_cd                    STRING,
    accounting_document_type_cd_nm STRING,
    CONSTRAINT pk_accounting_document_type
        PRIMARY KEY (accounting_document_type_id) NOT ENFORCED
)
USING DELTA
TBLPROPERTIES ('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2');

-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE atscale_geo_security (
    region STRING NOT NULL,
    role   STRING,
    CONSTRAINT pk_atscale_geo_security PRIMARY KEY (region) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE version_forecast_mapping (
    version_forecast_mapping_id BIGINT NOT NULL,
    version_nbr                 STRING,
    version_group_nm            STRING,
    active_ind                  STRING,
    CONSTRAINT pk_version_forecast_mapping
        PRIMARY KEY (version_forecast_mapping_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE functional_area (
    functional_area_id BIGINT NOT NULL,
    language_cd        STRING,
    functional_area_cd STRING,
    functional_area_nm STRING,
    CONSTRAINT pk_functional_area PRIMARY KEY (functional_area_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- CALENDAR
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE calendar_fiscal_period_v (
    fiscal_year_period_nbr           INT    NOT NULL,
    month_long_nm                    STRING,
    month_short_nm                   STRING,
    month_nbr                        INT,
    year_mth                         INT,
    month_relevance_dt               DATE,
    month_start_dt                   DATE,
    month_end_dt                     DATE,
    month_sort_sequence_nbr          INT,
    fiscal_period_nbr                INT,
    fiscal_period_cd                 STRING,
    fiscal_period_sort_sequence_nbr  INT,
    fiscal_year_period_cd            STRING,
    fiscal_year_period_nm            STRING,
    season_period_cd                 STRING,
    season_alternate_period_cd       STRING,
    season_nm                        STRING,
    season_relevance_dt              DATE,
    season_start_dt                  DATE,
    season_end_dt                    DATE,
    season_sort_sequence_nbr         INT,
    quarter_calendar_nbr             INT,
    quarter_calendar_sequence_nbr    INT,
    quarter_business_nbr             INT,
    fiscal_quarter_nbr               INT,
    fiscal_quarter_cd                STRING,
    fiscal_quarter_sort_sequence_nbr INT,
    fiscal_year_quarter_nbr          INT,
    fiscal_year_quarter_cd           STRING,
    fiscal_year_quarter_alternate_cd STRING,
    year_cd                          STRING,
    year_nm                          STRING,
    year_nbr                         STRING,
    year_start_dt                    DATE,
    year_end_dt                      DATE,
    business_year_nbr                INT,
    fiscal_year_nbr                  INT,
    fiscal_year_cd                   STRING,
    fiscal_period_sort               INT,
    CONSTRAINT pk_calendar_fiscal_period PRIMARY KEY (fiscal_year_period_nbr) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- COMPANY
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE company_code (
    company_id           BIGINT NOT NULL,
    company_cd           STRING,
    company_nm           STRING,
    currency_cd          STRING,
    created_by_user_id   STRING,
    updated_by_user_id   STRING,
    physical_source_cd   STRING,
    CONSTRAINT pk_company_code PRIMARY KEY (company_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- PRODUCT & CUSTOMER
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE finance_product_dim_v (
    product_id                                  BIGINT NOT NULL,
    primary_platform_desc                       STRING,
    style_nm                                    STRING,
    franchise_nm                                STRING,
    gender_desc                                 STRING,
    global_category_core_focus_desc             STRING,
    product_cd                                  STRING,
    team_nm                                     STRING,
    league_desc                                 STRING,
    athlete_full_nm                             STRING,
    product_company_nm                          STRING,
    age_desc                                    STRING,
    consumer_construct_dimension_nm             STRING,
    fields_of_play_nm                           STRING,
    merchandising_classification_desc           STRING,
    consumer_construct_segment_nm               STRING,
    brand_nm                                    STRING,
    sub_category_desc                           STRING,
    blank_usage_ind                             STRING,
    silhouette_desc                             STRING,
    silhouette_type_desc                        STRING,
    style_nbr                                   STRING,
    consumer_construct_global_consumer_offense_nm STRING,
    active_ind                                  STRING,
    created_by_user_id                          STRING,
    updated_by_user_id                          STRING,
    physical_source_cd                          STRING,
    global_sport_focus_derived_desc             STRING,
    global_sport_focus_desc                     STRING,
    global_sport_sub_focus_desc                 STRING,
    sub_brand_desc                              STRING,
    sub_brand_cd                                STRING,
    CONSTRAINT pk_finance_product_dim PRIMARY KEY (product_id) NOT ENFORCED
)
USING DELTA
TBLPROPERTIES ('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2');

-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE finance_customer_dim_v (
    finance_customer_id                          BIGINT NOT NULL,
    customer_nbr                                 STRING,
    channel_desc                                 STRING,
    customer_nm                                  STRING,
    customer_owner_group_nm                      STRING,
    marketplace_channel_nm                       STRING,
    geo_marketplace_unit_nm                      STRING,
    integrated_business_planning_level_1_desc    STRING,
    integrated_business_planning_level_2_desc    STRING,
    integrated_business_planning_level_3_desc    STRING,
    integrated_business_planning_mpu_desc        STRING,
    sub_territory_nm                             STRING,
    customer_business_type_nm                    STRING,
    customer_subtype_nm                          STRING,
    partner_channel                              STRING,
    partner_sub_channel                          STRING,
    partner_account_classification               STRING,
    CONSTRAINT pk_finance_customer_dim PRIMARY KEY (finance_customer_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- PROFIT CENTER
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE profit_center (
    profit_center_id        BIGINT NOT NULL,
    profit_center_nbr       STRING,
    profit_center_nm        STRING,
    segment_id              STRING,
    geography_nm            STRING,
    profit_center_channel_nm STRING,
    territory_nm            STRING,
    sub_territory_nm        STRING,
    begin_effective_dt      DATE,
    end_effective_dt        DATE,
    active_ind              STRING,
    geography_sort          INT,
    operating_segment_nm    STRING,
    CONSTRAINT pk_profit_center PRIMARY KEY (profit_center_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- DIVISION
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE division_text (
    division_id                   BIGINT NOT NULL,
    division_nbr                  VARCHAR(50),
    division_nm                   STRING,
    division_group                STRING,
    last_row_ind                  STRING,
    common_data_service_view_nm   STRING,
    source_system_nm              STRING,
    raw_tmst                      DATE,
    record_created_tmst_utc       DATE,
    record_update_tmst_utc        DATE,
    created_by_user_id            STRING,
    updated_by_user_id            STRING,
    physical_source_cd            STRING,
    language_cd                   STRING,
    active_ind                    STRING,
    CONSTRAINT pk_division_text PRIMARY KEY (division_id) NOT ENFORCED
)
USING DELTA;

CREATE OR REPLACE TABLE division_text_dim_v (
    division_nbr    VARCHAR(50) NOT NULL,
    division_nm     STRING,
    division_id     BIGINT,
    division_group  STRING,
    CONSTRAINT pk_division_text_dim_v PRIMARY KEY (division_nbr) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- COST CENTER
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE cost_center_dim_v (
    cost_center_nbr                           VARCHAR(255) NOT NULL,
    controlling_area_cd                       STRING,
    valid_to_dt                               DATE,
    valid_from_dt                             DATE,
    iso_language_cd                           STRING,
    cost_center_nm                            STRING,
    cost_center_desc                          STRING,
    cost_center_category_hierarchy_1_cd       STRING,
    cost_center_category_hierarchy_2_cd       STRING,
    company_cd                                STRING,
    source_system                             STRING,
    cost_center_type_cd                       STRING,
    cost_center_category_short_desc           STRING,
    business_area_cd                          STRING,
    tax_jurisdiction_cd                       STRING,
    functional_area_cd                        STRING,
    currency_cd                               STRING,
    posting_allowed_ind                       STRING,
    planning_allowed_ind                      STRING,
    secondary_costs_posting_allowed_ind       STRING,
    revenue_posting_allowed_ind               STRING,
    commitment_update_allowed_ind             STRING,
    secondary_costs_planning_allowed_ind      STRING,
    revenue_planning_allowed_ind              STRING,
    quantity_required_ind                     STRING,
    department_nm                             STRING,
    cost_center_report_printer_destination_cd STRING,
    company_legal_entity_id                   STRING,
    profit_center_nbr                         STRING,
    responsible_user_nm                       STRING,
    responsible_user_id                       STRING,
    responsible_user_title                    STRING,
    line_1_nm                                 STRING,
    line_2_nm                                 STRING,
    line_3_nm                                 STRING,
    line_4_nm                                 STRING,
    country_cd                                STRING,
    region_cd                                 STRING,
    city_nm                                   STRING,
    district_nm                               STRING,
    postal_cd                                 STRING,
    street_address_txt                        STRING,
    po_box_postal_cd                          STRING,
    po_box_nbr                                STRING,
    correspondence_language_cd                STRING,
    first_telephone_nbr                       STRING,
    second_telephone_nbr                      STRING,
    telebox_nbr                               STRING,
    fax_nbr                                   STRING,
    teletex_nbr                               STRING,
    telex_nbr                                 STRING,
    data_communication_line_nbr               STRING,
    msg_header_tmst                           DATE,
    begin_effective_dt                        DATE,
    end_effective_dt                          DATE,
    cost_center_id                            BIGINT,
    _cost_center_cleansed_latest_load_timestamp DATE,
    CONSTRAINT pk_cost_center_dim_v PRIMARY KEY (cost_center_nbr) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- GL ACCOUNT
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE gl_account_dim (
    gl_account_nbr      VARCHAR(255) NOT NULL,
    gl_account_short_desc STRING,
    gl_account_long_desc  STRING,
    begin_effective_dt    DATE,
    end_effective_dt      DATE,
    active_ind            STRING,
    gl_accnt_id           BIGINT,
    cost_component_calc   STRING,
    CONSTRAINT pk_gl_account_dim PRIMARY KEY (gl_account_nbr) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- GEO CHANNEL DIMS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE geo_marketplace_channel_dim (
    geo_marketplace_channel_id BIGINT NOT NULL,
    geo_marketplace_channel_nm STRING,
    created_by_user_id         STRING,
    updated_by_user_id         STRING,
    physical_source_cd         STRING,
    active_ind                 STRING,
    CONSTRAINT pk_geo_marketplace_channel PRIMARY KEY (geo_marketplace_channel_id) NOT ENFORCED
)
USING DELTA;

CREATE OR REPLACE TABLE geo_wholesale_value_business_dim (
    geo_wholesale_value_business_id   BIGINT NOT NULL,
    geo_wholesale_value_business_desc STRING,
    created_by_user_id                STRING,
    updated_by_user_id                STRING,
    physical_source_cd                STRING,
    CONSTRAINT pk_geo_wholesale_value_business PRIMARY KEY (geo_wholesale_value_business_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- ZFSM MEASURES
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE gl_account_zfsm_measures_hierarchy_dim (
    zfsm_measure_id         BIGINT NOT NULL,
    created_by_user_id      STRING,
    updated_by_user_id      STRING,
    physical_source_cd      STRING,
    active_ind              STRING,
    zfsm_measure_cd         STRING,
    zfsm_measure_desc       STRING,
    gl_account_level_1_cd   STRING,
    gl_account_level_1_nm   STRING,
    gl_account_level_2_cd   STRING,
    gl_account_level_2_nm   STRING,
    gl_account_level_3_cd   STRING,
    gl_account_level_3_nm   STRING,
    gl_account_level_4_cd   STRING,
    gl_account_level_4_nm   STRING,
    gl_account_level_5_cd   STRING,
    gl_account_level_5_nm   STRING,
    gl_account_level_6_cd   STRING,
    gl_account_level_6_nm   STRING,
    gl_account_level_7_cd   STRING,
    gl_account_level_7_nm   STRING,
    gl_account_level_8_cd   STRING,
    gl_account_level_8_nm   STRING,
    gl_account_level_9_cd   STRING,
    gl_account_level_9_nm   STRING,
    gl_account_level_10_cd  STRING,
    gl_account_level_10_nm  STRING,
    gl_account_level_11_cd  STRING,
    gl_account_level_11_nm  STRING,
    gl_account_level_12_cd  STRING,
    gl_account_level_12_nm  STRING,
    gl_account_level_13_cd  STRING,
    gl_account_level_13_nm  STRING,
    record_created_tmst_utc DATE,
    record_update_tmst_utc  DATE,
    CONSTRAINT pk_zfsm_measures PRIMARY KEY (zfsm_measure_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- COPA ATTRIBUTION
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE copa_attribution_dim (
    copa_attribution_id               BIGINT NOT NULL,
    responsive_business_model_cd      STRING,
    responsive_business_model_desc    STRING,
    demand_stream_cd                  STRING,
    demand_stream_desc                STRING,
    business_type_cd                  STRING,
    business_type_desc                STRING,
    marketing_type_cd                 STRING,
    marketing_type_desc               STRING,
    gender_age_cd                     STRING,
    gender_age_desc                   STRING,
    direct_business_model_cd          STRING,
    direct_business_model_desc        STRING,
    product_lifecycle_cd              STRING,
    product_lifecycle_desc            STRING,
    quality_cd                        STRING,
    quality_desc                      STRING,
    region_summary_product_group_cd   STRING,
    region_summary_product_group_desc STRING,
    sales_order_reason_desc           STRING,
    sales_order_type_cd               STRING,
    sales_order_type_desc             STRING,
    sales_order_type_group_desc       STRING,
    sales_order_item_category_cd      STRING,
    sales_order_item_category_desc    STRING,
    distribution_method_cd            STRING,
    distribution_method_desc          STRING,
    sales_order_reason_cd             STRING,
    CONSTRAINT pk_copa_attribution PRIMARY KEY (copa_attribution_id) NOT ENFORCED
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- GENERAL LEDGER FACT  ← central fact table
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE general_ledger_fact (
    general_ledger_fact_id                    BIGINT          NOT NULL,
    fiscal_year_period_nbr                    INT,
    profit_center_id                          BIGINT,
    division_id                               BIGINT,
    version_forecast_mapping_id               BIGINT,
    functional_area_id                        BIGINT,
    accounting_document_type_id               BIGINT,
    product_id                                BIGINT,
    customer_id                               BIGINT,
    company_id                                BIGINT,
    copa_attribution_id                       BIGINT,
    cost_center_nbr                           STRING,
    geo_wholesale_value_business_id           BIGINT,
    geo_marketplace_channel_id                BIGINT,
    gl_account_nbr                            STRING,
    zfsm_measure_id                           BIGINT,
    company_currency_amt                      DECIMAL(28,5),
    transaction_currency_amt                  DECIMAL(28,5),
    performance_management_currency_amt       DECIMAL(28,5),
    etm_ind                                   INT,
    etm_foreign_currency_exchange_rate_id     BIGINT,
    gaap_foreign_currency_exchange_rate_id    BIGINT,
    sales_qty                                 DECIMAL(28,5),
    returns_qty                               DECIMAL(28,5),
    general_ledger_fact_ind                   STRING,
    cis_delta_ind                             STRING,
    general_ledger_ocogs_allocation_fact_ind  STRING,
    anaplan_corporate_ind                     STRING,
    company_currency_cd                       STRING,
    transaction_currency_cd                   STRING,
    CONSTRAINT pk_general_ledger_fact PRIMARY KEY (general_ledger_fact_id) NOT ENFORCED,
    CONSTRAINT fk_gl_accounting_document_type
        FOREIGN KEY (accounting_document_type_id)
        REFERENCES accounting_document_type (accounting_document_type_id) NOT ENFORCED,
    CONSTRAINT fk_gl_calendar_fiscal_period
        FOREIGN KEY (fiscal_year_period_nbr)
        REFERENCES calendar_fiscal_period_v (fiscal_year_period_nbr) NOT ENFORCED,
    CONSTRAINT fk_gl_profit_center
        FOREIGN KEY (profit_center_id)
        REFERENCES profit_center (profit_center_id) NOT ENFORCED,
    CONSTRAINT fk_gl_division
        FOREIGN KEY (division_id)
        REFERENCES division_text (division_id) NOT ENFORCED,
    CONSTRAINT fk_gl_version_forecast_mapping
        FOREIGN KEY (version_forecast_mapping_id)
        REFERENCES version_forecast_mapping (version_forecast_mapping_id) NOT ENFORCED,
    CONSTRAINT fk_gl_functional_area
        FOREIGN KEY (functional_area_id)
        REFERENCES functional_area (functional_area_id) NOT ENFORCED,
    CONSTRAINT fk_gl_product
        FOREIGN KEY (product_id)
        REFERENCES finance_product_dim_v (product_id) NOT ENFORCED,
    CONSTRAINT fk_gl_customer
        FOREIGN KEY (customer_id)
        REFERENCES finance_customer_dim_v (finance_customer_id) NOT ENFORCED,
    CONSTRAINT fk_gl_company
        FOREIGN KEY (company_id)
        REFERENCES company_code (company_id) NOT ENFORCED,
    CONSTRAINT fk_gl_copa_attribution
        FOREIGN KEY (copa_attribution_id)
        REFERENCES copa_attribution_dim (copa_attribution_id) NOT ENFORCED,
    CONSTRAINT fk_gl_geo_wholesale
        FOREIGN KEY (geo_wholesale_value_business_id)
        REFERENCES geo_wholesale_value_business_dim (geo_wholesale_value_business_id) NOT ENFORCED,
    CONSTRAINT fk_gl_geo_marketplace_channel
        FOREIGN KEY (geo_marketplace_channel_id)
        REFERENCES geo_marketplace_channel_dim (geo_marketplace_channel_id) NOT ENFORCED
)
USING DELTA
PARTITIONED BY (fiscal_year_period_nbr)
TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);

-- ---------------------------------------------------------------------------
-- CIS FACT
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CIS_fact (
    gl_account_id                              BIGINT NOT NULL,
    profit_center_nbr                          STRING,
    fiscal_year_period_nbr                     INT,
    fiscal_yr                                  INT,
    transaction_currency_amt                   DECIMAL(18,5),
    functional_area_cd                         STRING,
    profit_center_id                           BIGINT,
    division_nbr                               STRING,
    segment_nbr                                INT,
    partner_segment_nbr                        INT,
    document_type_cd                           STRING,
    original_company_cd                        INT,
    sign_adjusted_group_currency_amt           DECIMAL(18,5),
    sign_adjusted_local_currency_amt           DECIMAL(18,5),
    sign_adjusted_transaction_currency_amt     DECIMAL(18,5),
    group_currency_amt                         DECIMAL(18,5),
    local_currency_amt                         DECIMAL(18,5),
    qty                                        DECIMAL(18,5),
    consolidated_income_statement_fact_id      BIGINT,
    financial_statement_item_cd                STRING,
    local_currency_cd                          STRING,
    transaction_currency_cd                    STRING,
    version_nbr                                STRING,
    partner_profit_center_nbr                  STRING,
    partner_unit_cd                            STRING,
    consolidation_unit_cd                      INT,
    ledger_cd                                  STRING,
    dimension_cd                               STRING,
    record_type_cd                             STRING,
    consolidation_group_cd                     STRING,
    consolidation_of_investment_activity_nbr   INT,
    chart_of_accounts_cd                       STRING,
    trading_partner_nbr                        INT,
    region_summary_product_group_cd            STRING,
    version_group_nm                           STRING,
    consolidated_segment_nm                    STRING,
    sign_adjusted_qty                          DECIMAL(18,5),
    user_nm                                    STRING,
    additional_operation_information_nm        STRING,
    created_by_user_id                         STRING,
    updated_by_user_id                         STRING,
    physical_source_cd                         STRING,
    cis_store_cd                               STRING,
    posting_level_cd                           STRING,
    base_unit_of_measure_cd                    STRING,
    foreign_exchange_type_cd                   STRING,
    consolidated_channel_nm                    STRING,
    CONSTRAINT pk_CIS_fact PRIMARY KEY (gl_account_id) NOT ENFORCED
)
USING DELTA
PARTITIONED BY (fiscal_year_period_nbr);

-- ---------------------------------------------------------------------------
-- CONSOLIDATED BALANCE SHEET FACT
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE consolidated_balance_sheet_fact (
    consolidated_balance_sheet_fact_id  BIGINT NOT NULL,
    financial_statement_item_cd         STRING,
    profit_center_nbr                   STRING,
    functional_area_cd                  STRING,
    local_currency_cd                   STRING,
    transaction_currency_cd             STRING,
    version_nbr                         STRING,
    division_nbr                        STRING,
    fiscal_year_period_nbr              INT,
    partner_unit_cd                     STRING,
    posting_level_cd                    STRING,
    document_type_cd                    STRING,
    consolidation_unit_cd               INT,
    partner_profit_center_nbr           STRING,
    trading_partner_nbr                 INT,
    region_summary_product_group_cd     STRING,
    transaction_currency_amt            DECIMAL(18,5),
    local_currency_amt                  DECIMAL(18,5),
    group_currency_amt                  DECIMAL(18,5),
    partner_segment_nbr                 INT,
    segment_nbr                         STRING,
    qty                                 DECIMAL(18,5),
    consolidated_segment_nm             STRING,
    consolidated_channel_nm             STRING,
    fiscal_yr                           INT,
    version_group_nm                    STRING,
    user_nm                             STRING,
    additional_operation_information_nm STRING,
    created_by_user_id                  STRING,
    updated_by_user_id                  STRING,
    physical_source_cd                  STRING,
    _acdocu_latest_load_timestamp       DATE,
    group_currency_cd                   STRING,
    ending_balance_amt                  DECIMAL(18,5),
    foreign_exchange_type_cd            STRING,
    CONSTRAINT pk_consolidated_balance_sheet_fact
        PRIMARY KEY (consolidated_balance_sheet_fact_id) NOT ENFORCED
)
USING DELTA
PARTITIONED BY (fiscal_year_period_nbr);

-- ---------------------------------------------------------------------------
-- POST-GENERATION OPTIMISATION  (run after data load)
-- ---------------------------------------------------------------------------
-- OPTIMIZE development.dev_pbi_perform_cf_poc.general_ledger_fact
--     ZORDER BY (profit_center_id, gl_account_nbr, product_id);

-- OPTIMIZE development.dev_pbi_perform_cf_poc.CIS_fact
--     ZORDER BY (profit_center_id);

-- OPTIMIZE development.dev_pbi_perform_cf_poc.consolidated_balance_sheet_fact
--     ZORDER BY (financial_statement_item_cd);
