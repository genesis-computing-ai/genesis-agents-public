
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS HCLS;

-- Create schema if not exists 
CREATE SCHEMA IF NOT EXISTS HCLS.BRONZE;

-- Raw Claims Header Information
CREATE TABLE HCLS.BRONZE.RAW_CLAIM_HEADERS (
    claim_id VARCHAR(20),
    patient_id VARCHAR(15),
    filing_date VARCHAR(10),
    adjudication_date VARCHAR(10),
    claim_status VARCHAR(20),
    claim_type VARCHAR(30),
    authorization_number VARCHAR(20),
    network_indicator VARCHAR(1),
    total_billed_amt NUMBER(15,2),
    total_paid_amt NUMBER(15,2),
    total_allowed_amt NUMBER(15,2),
    total_member_liability NUMBER(15,2),
    raw_source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Raw Claims Line Items
CREATE TABLE HCLS.BRONZE.RAW_CLAIM_LINES (
    claim_id VARCHAR(20),
    claim_line_number INTEGER,
    svc_date VARCHAR(10),  -- Often inconsistent date formats in source
    provider_id VARCHAR(15),
    proc_cd VARCHAR(10),
    pos_cd VARCHAR(5),
    revenue_code VARCHAR(4),
    units NUMBER(5),
    billed_amt NUMBER(15,2),
    paid_amt NUMBER(15,2),
    allowed_amt NUMBER(15,2),
    copay_amt NUMBER(15,2),
    deductible_amt NUMBER(15,2),
    raw_source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Raw Claims Diagnosis Information
CREATE TABLE HCLS.BRONZE.RAW_CLAIM_DIAGNOSES (
    claim_id VARCHAR(20),
    diagnosis_sequence INTEGER,
    diag_cd VARCHAR(10),
    diag_type VARCHAR(10),  -- Primary, Secondary, Admitting, etc.
    poa_indicator VARCHAR(1),  -- Present on Admission
    raw_source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Raw Claims Pharmacy Details
CREATE TABLE HCLS.BRONZE.RAW_CLAIM_PHARMACY (
    claim_id VARCHAR(20),
    claim_line_number INTEGER,
    ndc_code VARCHAR(11),
    drug_name VARCHAR(100),
    drug_strength VARCHAR(50),
    dosage_form VARCHAR(50),
    days_supply INTEGER,
    quantity_dispensed NUMBER(10,3),
    refill_number INTEGER,
    prescribing_provider_id VARCHAR(15),
    pharmacy_id VARCHAR(15),
    daw_code VARCHAR(1),
    compound_code VARCHAR(2),
    raw_source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Raw Claims Payment Adjustments
CREATE TABLE HCLS.BRONZE.RAW_CLAIM_ADJUSTMENTS (
    claim_id VARCHAR(20),
    claim_line_number INTEGER,
    adjustment_sequence INTEGER,
    adjustment_type VARCHAR(30),
    adjustment_reason_code VARCHAR(10),
    adjustment_amount NUMBER(15,2),
    adjustment_date VARCHAR(10),
    processed_by VARCHAR(50),
    raw_source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Provider Information
CREATE TABLE HCLS.BRONZE.RAW_PROVIDERS (
    provider_id VARCHAR(15),
    npi VARCHAR(10),
    provider_name VARCHAR(100),
    specialty_code VARCHAR(10),
    specialty_desc VARCHAR(100),
    tax_id VARCHAR(9),
    practice_address VARCHAR(200),
    practice_city VARCHAR(100),
    practice_state VARCHAR(2),
    practice_zip VARCHAR(10),
    provider_status VARCHAR(20),
    network_tier VARCHAR(20),
    contract_start_date VARCHAR(10),
    contract_end_date VARCHAR(10),
    credentialing_date VARCHAR(10),
    board_certified_flag VARCHAR(1),
    medical_school VARCHAR(100),
    graduation_year VARCHAR(4),
    source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Diagnosis Reference Data
CREATE TABLE HCLS.BRONZE.RAW_DIAGNOSIS_CODES (
    diagnosis_code VARCHAR(10),
    icd_version VARCHAR(5),
    code_description VARCHAR(255),
    category_code VARCHAR(10),
    category_description VARCHAR(100),
    subcategory_code VARCHAR(10),
    subcategory_description VARCHAR(100),
    billable_flag VARCHAR(1),
    effective_date VARCHAR(10),
    termination_date VARCHAR(10),
    severity_level VARCHAR(2),
    chronic_condition_flag VARCHAR(1),
    source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Place of Service Reference
CREATE TABLE HCLS.BRONZE.RAW_PLACE_OF_SERVICE (
    pos_code VARCHAR(5),
    pos_description VARCHAR(100),
    pos_category VARCHAR(50),
    facility_based_flag VARCHAR(1),
    emergency_flag VARCHAR(1),
    institutional_flag VARCHAR(1),
    effective_date VARCHAR(10),
    termination_date VARCHAR(10),
    source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
);

-- Procedure Codes and Fee Schedule
CREATE TABLE HCLS.BRONZE.RAW_PROCEDURES (
    procedure_code VARCHAR(10),
    proc_code_type VARCHAR(10),  -- CPT, HCPCS, etc.
    proc_description VARCHAR(255),
    proc_category VARCHAR(50),
    proc_subcategory VARCHAR(50),
    standard_fee NUMBER(15,2),
    relative_value_units NUMBER(8,2),
    facility_fee NUMBER(15,2),
    non_facility_fee NUMBER(15,2),
    effective_date VARCHAR(10),
    termination_date VARCHAR(10),
    prior_auth_required VARCHAR(1),
    minimum_age NUMBER(3),
    maximum_age NUMBER(3),
    gender_restriction VARCHAR(1),
    source_system VARCHAR(10),
    load_timestamp TIMESTAMP_NTZ
); 