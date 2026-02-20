-- ============================================================
-- PALTAC_HANDSON セットアップスクリプト
-- データベース・ウェアハウス・スキーマの作成
-- GitHubからCSVを取得し、3テーブルを作成
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- STEP 1: クロスリージョン推論を有効化
-- ============================================================
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ============================================================
-- STEP 2: ウェアハウスの作成
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for PALTAC Handson';

-- ============================================================
-- STEP 3: データベースの作成
-- ============================================================
CREATE DATABASE IF NOT EXISTS PALTAC_HANDSON;
USE DATABASE PALTAC_HANDSON;

-- ============================================================
-- STEP 4: スキーマの作成
-- ============================================================
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = '生データ格納用スキーマ';
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = '分析・AI処理結果格納用スキーマ';

SHOW SCHEMAS IN DATABASE PALTAC_HANDSON;

-- ============================================================
-- STEP 5: ステージの作成
-- ============================================================
USE SCHEMA PALTAC_HANDSON.RAW;

CREATE OR REPLACE STAGE PALTAC_HANDSON.RAW.HANDSON_RESOURCES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for PALTAC Handson resources';

-- ============================================================
-- STEP 6: GitHub連携 — Git Integrationの作成
-- ============================================================
CREATE OR REPLACE API INTEGRATION paltac_handson_git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY PALTAC_HANDSON.RAW.GIT_PALTAC_HANDSON
    API_INTEGRATION = paltac_handson_git_api_integration
    ORIGIN = 'https://github.com/hwatari-snow/Paltac_handson.git';

-- リポジトリの内容確認
LIST @PALTAC_HANDSON.RAW.GIT_PALTAC_HANDSON/branches/main;

-- ============================================================
-- STEP 7: GitHubからCSVをステージにコピー
-- ============================================================
COPY FILES INTO @PALTAC_HANDSON.RAW.HANDSON_RESOURCES/csv/
    FROM @PALTAC_HANDSON.RAW.GIT_PALTAC_HANDSON/branches/main/csv/
    PATTERN = '.*\.csv$';

-- コピーされたファイルの確認
LIST @PALTAC_HANDSON.RAW.HANDSON_RESOURCES/csv/;

-- ============================================================
-- STEP 8: CSVファイルフォーマットの作成
-- ============================================================
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE FILE FORMAT PALTAC_HANDSON.RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    ENCODING = 'UTF8';

-- ============================================================
-- STEP 9: 商品マスタテーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE PALTAC_HANDSON.RAW.PRODUCT_MASTER (
    JANCODE       VARCHAR(20)  NOT NULL,
    PRODUCT_NAME  VARCHAR(500) NOT NULL,
    MAKER_NAME    VARCHAR(100),
    MAKER_CODE    VARCHAR(20)
);

COPY INTO PALTAC_HANDSON.RAW.PRODUCT_MASTER
    (JANCODE, PRODUCT_NAME, MAKER_NAME, MAKER_CODE)
FROM @PALTAC_HANDSON.RAW.HANDSON_RESOURCES/csv/product_master.csv
FILE_FORMAT = (FORMAT_NAME = 'PALTAC_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM PALTAC_HANDSON.RAW.PRODUCT_MASTER;
SELECT * FROM PALTAC_HANDSON.RAW.PRODUCT_MASTER LIMIT 10;

-- ============================================================
-- STEP 10: POSデータ（売上データ）テーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE PALTAC_HANDSON.ANALYTICS.POS_DATA (
    SALE_DATE     DATE         NOT NULL,
    JANCODE       VARCHAR(20)  NOT NULL,
    PRODUCT_NAME  VARCHAR(500),
    MAKER_NAME    VARCHAR(100),
    SALES_QTY     NUMBER(10,0),
    UNIT_PRICE    NUMBER(10,0),
    SALES_AMOUNT  NUMBER(15,0)
);

COPY INTO PALTAC_HANDSON.ANALYTICS.POS_DATA
    (SALE_DATE, JANCODE, PRODUCT_NAME, MAKER_NAME, SALES_QTY, UNIT_PRICE, SALES_AMOUNT)
FROM @PALTAC_HANDSON.RAW.HANDSON_RESOURCES/csv/pos_data.csv
FILE_FORMAT = (FORMAT_NAME = 'PALTAC_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM PALTAC_HANDSON.ANALYTICS.POS_DATA;
SELECT * FROM PALTAC_HANDSON.ANALYTICS.POS_DATA LIMIT 10;

-- ============================================================
-- STEP 11: 在庫データテーブルの作成とロード
-- ============================================================
CREATE OR REPLACE TABLE PALTAC_HANDSON.ANALYTICS.INVENTORY (
    SNAPSHOT_DATE  DATE         NOT NULL,
    JANCODE        VARCHAR(20)  NOT NULL,
    PRODUCT_NAME   VARCHAR(500),
    MAKER_NAME     VARCHAR(100),
    STOCK_QTY      NUMBER(10,0),
    UNIT_PRICE     NUMBER(10,0),
    STOCK_AMOUNT   NUMBER(15,0),
    STOCK_STATUS   VARCHAR(20)
);

COPY INTO PALTAC_HANDSON.ANALYTICS.INVENTORY
    (SNAPSHOT_DATE, JANCODE, PRODUCT_NAME, MAKER_NAME, STOCK_QTY, UNIT_PRICE, STOCK_AMOUNT, STOCK_STATUS)
FROM @PALTAC_HANDSON.RAW.HANDSON_RESOURCES/csv/inventory_data.csv
FILE_FORMAT = (FORMAT_NAME = 'PALTAC_HANDSON.RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS TOTAL_RECORDS FROM PALTAC_HANDSON.ANALYTICS.INVENTORY;
SELECT * FROM PALTAC_HANDSON.ANALYTICS.INVENTORY LIMIT 10;

-- ============================================================
-- STEP 12: データ確認サマリ
-- ============================================================
SELECT 'PRODUCT_MASTER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM PALTAC_HANDSON.RAW.PRODUCT_MASTER
UNION ALL
SELECT 'POS_DATA',                     COUNT(*)              FROM PALTAC_HANDSON.ANALYTICS.POS_DATA
UNION ALL
SELECT 'INVENTORY',                    COUNT(*)              FROM PALTAC_HANDSON.ANALYTICS.INVENTORY
ORDER BY TABLE_NAME;
