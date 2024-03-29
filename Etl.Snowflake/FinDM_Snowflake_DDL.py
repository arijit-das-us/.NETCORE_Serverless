import os
import sys
import snowflake.connector

if len(sys.argv) != 6:
    print("==> Incorrect # of parms. Usage: ",__file__," SnowflakeAccount Username Password Role Warehouse ")
    sys.exit(1)

sfAccount = sys.argv[1]
sfUser = sys.argv[2]
sfPswd = sys.argv[3]
sfPswd =''

if sfPswd == '':
    import getpass
    sfPswd = getpass.getpass('Password:')

sfRole = sys.argv[4]
sfWarehouse = sys.argv[5]

# Create Database MEDINSIGHT
def create_db(sfConnect):
    cur = sfConnect.cursor()
    cur.execute("create database if not exists FIN_DM")
    print("==> created database: FIN_DM")
    cur.close()
    
# Create Schema DBO
def create_schema(sfConnect):
    cur = sfConnect.cursor()
    cur.execute("create schema if not exists DBO")
    print("==> created schema: DBO")
    cur.close()

# Create Warehouse MEDINSIGHT_WH
def create_warehouse(sfConnect):
    cur = sfConnect.cursor()
    cur.execute("""
create warehouse if not exists FIN_DM_WH
  server_type=standard,
  server_count=2
""")
    print("==> created warehouse: FIN_DM_WH")
    cur.close()

# Create Tables
def create_tables(sfConnect):
    cur = sfConnect.cursor()

    cur.execute("""Create Or Replace Table DBO.FNCL_LOB_D (
    FNCL_LOB_SK                    INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    FNCL_LOB_CD                    VARCHAR(20)         NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    FNCL_LOB_DESC                  VARCHAR(70)                      ,
    FNCL_LOB_EFF_DT_SK             CHAR(10)            NOT NULL     ,
    FNCL_LOB_STTUS_CD              VARCHAR(20)                      ,
    FNCL_LOB_STTUS_NM              VARCHAR(255)                     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    FNCL_LOB_STTUS_CD_SK           INTEGER             NOT NULL     ,
    SPIRA_BNF_ID                   VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    FNCL_MKT_SEG_NM                VARCHAR(256)                     ,
    FNCL_RPTNG_SEG_NM              VARCHAR(256)                     ,
    QFR_GRP_NM                     VARCHAR(256)                     ,
    STATUTORY_GRP_NM               VARCHAR(256)                     ,
    ALLOC_ST_NM                    VARCHAR(256)                     ,
    CO_SK                          INTEGER                          ,
    BCBSA_NTNL_ACCT_IN             CHAR(1)                          ,
    SPIRA_BNF_IN                   CHAR(1)                          ,
    LAST_UPDT_DT_SK                CHAR(10)                         ,
    ACA_STTUS_NM                   VARCHAR(255)                     ,
    FCTS_STTUS_NM                  VARCHAR(255)                     ,
    FNCL_FUND_TYP_NM               VARCHAR(255)                     ,
    FNCL_GRP_SIZE_CAT_NM           VARCHAR(255)                     ,
    LOB_NM                         VARCHAR(255)                     ,
    MCARE_ADVNTG_PROD_TYP_NM       VARCHAR(255)                     ,
    REF_ID                         VARCHAR(20)                      ,
    REL_INT_LOB_NM                 VARCHAR(255)                     ) ;""")
    print("==> created Table: FNCL_LOB_D")

    cur.execute("""Create Or Replace Table DBO.GRP_D (
    GRP_SK                         INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    GRP_ID                         VARCHAR(20)         NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    MKT_SEG_SK                     INTEGER             NOT NULL     ,
    CUR_GRP_SRVY_RSPN_DT_SK        CHAR(10)            NOT NULL     ,
    CUR_GRP_SRVY_RSPN_GRP_SIZE_NO  INTEGER             NOT NULL     ,
    GRP_BILL_LVL_CD                VARCHAR(20)         NOT NULL     ,
    GRP_BILL_LVL_NM                VARCHAR(255)                     ,
    GRP_BUS_SUB_CAT_SH_NM_CD       VARCHAR(20)         NOT NULL     ,
    GRP_BUS_SUB_CAT_SH_NM          VARCHAR(255)                     ,
    GRP_CAP_IN                     CHAR(1)             NOT NULL     ,
    GRP_CLNT_ID                    VARCHAR(20)         NOT NULL     ,
    GRP_CLNT_NM                    VARCHAR(75)                      ,
    GRP_CNTCT_FIRST_NM             VARCHAR(35)                      ,
    GRP_CNTCT_MIDINIT              CHAR(1)                          ,
    GRP_CNTCT_LAST_NM              VARCHAR(75)                      ,
    GRP_CNTCT_TTL                  VARCHAR(70)                      ,
    GRP_CUR_ANNV_DT_SK             CHAR(10)            NOT NULL     ,
    GRP_DP_IN                      CHAR(1)             NOT NULL     ,
    GRP_EDI_ACCT_VNDR_CD           VARCHAR(20)         NOT NULL     ,
    GRP_EDI_ACCT_VNDR_NM           VARCHAR(255)                     ,
    GRP_MKT_SIZE_CAT_CD            VARCHAR(20)         NOT NULL     ,
    GRP_MKT_SIZE_CAT_NM            VARCHAR(255)                     ,
    GRP_MKTNG_TERR_ID              VARCHAR(20)         NOT NULL     ,
    GRP_MULTI_OPT_IN               CHAR(1)             NOT NULL     ,
    GRP_NM                         VARCHAR(75)                      ,
    GRP_ADDR_LN_1                  VARCHAR(40)                      ,
    GRP_ADDR_LN_2                  VARCHAR(40)                      ,
    GRP_ADDR_LN_3                  VARCHAR(40)                      ,
    GRP_CITY_NM                    VARCHAR(35)                      ,
    GRP_ST_CD                      VARCHAR(20)         NOT NULL     ,
    GRP_ST_NM                      VARCHAR(255)                     ,
    GRP_ZIP_CD_5                   CHAR(5)                          ,
    GRP_ZIP_CD_4                   CHAR(4)                          ,
    GRP_CNTY_NM                    VARCHAR(35)                      ,
    GRP_EMAIL_ADDR                 VARCHAR(75)                      ,
    GRP_PHN_NO                     VARCHAR(20)                      ,
    GRP_PHNEXT_NO                  VARCHAR(5)                       ,
    GRP_FAX_NO                     VARCHAR(20)                      ,
    GRP_FAX_EXT_NO                 VARCHAR(5)                       ,
    GRP_NEXT_ANNV_DT_SK            CHAR(10)            NOT NULL     ,
    GRP_ORIG_EFF_DT_SK             CHAR(10)            NOT NULL     ,
    GRP_REINST_DT_SK               CHAR(10)            NOT NULL     ,
    GRP_RNWL_DT_SK                 CHAR(10)            NOT NULL     ,
    GRP_RNWL_DT_MO_DAY             CHAR(4)                          ,
    GRP_STTUS_CD                   VARCHAR(20)         NOT NULL     ,
    GRP_STTUS_NM                   VARCHAR(255)                     ,
    GRP_TAX_ID_NO                  VARCHAR(20)                      ,
    GRP_TERM_DT_SK                 CHAR(10)            NOT NULL     ,
    GRP_TERM_RSN_CD                VARCHAR(20)         NOT NULL     ,
    GRP_TERM_RSN_NM                VARCHAR(255)                     ,
    GRP_TOT_CNTR_CT                INTEGER             NOT NULL     ,
    GRP_TOT_EMPL_CT                INTEGER             NOT NULL     ,
    GRP_TOT_ELIG_EMPL_CT           INTEGER             NOT NULL     ,
    GRP_UNIQ_KEY                   INTEGER             NOT NULL     ,
    PRNT_GRP_CLNT_ID               VARCHAR(20)         NOT NULL     ,
    PRNT_GRP_CLNT_NM               VARCHAR(75)                      ,
    PRNT_GRP_ID                    VARCHAR(20)         NOT NULL     ,
    PRNT_GRP_NM                    VARCHAR(75)                      ,
    PRNT_GRP_ADDR_LN_1             VARCHAR(40)                      ,
    PRNT_GRP_ADDR_LN_2             VARCHAR(40)                      ,
    PRNT_GRP_ADDR_LN_3             VARCHAR(40)                      ,
    PRNT_GRP_CITY_NM               VARCHAR(35)                      ,
    PRNT_GRP_ST_CD                 VARCHAR(20)         NOT NULL     ,
    PRNT_GRP_ST_NM                 VARCHAR(255)                     ,
    PRNT_GRP_ZIP_CD_5              CHAR(5)                          ,
    PRNT_GRP_ZIP_CD_4              CHAR(4)                          ,
    PRNT_GRP_CNTY_NM               VARCHAR(35)                      ,
    PRNT_GRP_PHN_NO                VARCHAR(20)                      ,
    PRNT_GRP_PHNEXT_NO             CHAR(5)                          ,
    PRNT_GRP_FAX_NO                VARCHAR(20)                      ,
    PRNT_GRP_FAX_EXT_NO            CHAR(5)                          ,
    PRNT_GRP_EMAIL_ADDR_TX         VARCHAR(75)                      ,
    PRNT_GRP_BUS_CAT_CD            VARCHAR(20)         NOT NULL     ,
    PRNT_GRP_BUS_CAT_NM            VARCHAR(255)                     ,
    PRNT_GRP_SIC_NACIS_CD          VARCHAR(20)         NOT NULL     ,
    PRNT_GRP_SIC_NACIS_NM          VARCHAR(255)                     ,
    PRNT_GRP_UNIQ_KEY              INTEGER             NOT NULL     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    GRP_BILL_LVL_CD_SK             INTEGER             NOT NULL     ,
    GRP_BUS_SUB_CAT_SH_NM_CD_SK    INTEGER             NOT NULL     ,
    GRP_CLNT_SK                    INTEGER             NOT NULL     ,
    GRP_EDI_ACCT_VNDR_CD_SK        INTEGER             NOT NULL     ,
    GRP_MKT_SIZE_CAT_CD_SK         INTEGER             NOT NULL     ,
    GRP_ST_CD_SK                   INTEGER             NOT NULL     ,
    GRP_STTUS_CD_SK                INTEGER             NOT NULL     ,
    GRP_TERM_RSN_CD_SK             INTEGER             NOT NULL     ,
    MKTNG_TERR_SK                  INTEGER             NOT NULL     ,
    PRNT_GRP_SK                    INTEGER             NOT NULL     ,
    PRNT_GRP_CLNT_SK               INTEGER             NOT NULL     ,
    PRNT_GRP_BUS_CAT_CD_SK         INTEGER             NOT NULL     ,
    PRNT_GRP_SIC_NACIS_CD_SK       INTEGER             NOT NULL     ,
    PRNT_GRP_ST_CD_SK              INTEGER             NOT NULL     ,
    WEB_PHYS_BNF_IN                CHAR(1)             NOT NULL     );  """)
    print("==> created Table: GRP_D")

    cur.execute("""Create Or Replace Table DBO.MBR_D (
    MBR_SK                         INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    MBR_UNIQ_KEY                   INTEGER             NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    CLS_SK                         INTEGER             NOT NULL     ,
    GRP_SK                         INTEGER             NOT NULL     ,
    SUBGRP_SK                      INTEGER             NOT NULL     ,
    SUB_ALPHA_PFX_SK               INTEGER             NOT NULL     ,
    SUB_MBR_SK                     INTEGER             NOT NULL     ,
    SUB_SK                         INTEGER             NOT NULL     ,
    CLS_DESC                       VARCHAR(70)                      ,
    CLS_ID                         VARCHAR(20)         NOT NULL     ,
    GRP_ID                         VARCHAR(20)         NOT NULL     ,
    GRP_NM                         VARCHAR(75)                      ,
    MBR_BRTH_DT_SK                 CHAR(10)            NOT NULL     ,
    MBR_CASEHEAD_ID                VARCHAR(20)         NOT NULL     ,
    MBR_DCSD_DT_SK                 CHAR(10)            NOT NULL     ,
    MBR_DCSD_IN                    CHAR(1)             NOT NULL     ,
    MBR_DSBLTY_COV_EFF_DT_SK       CHAR(10)            NOT NULL     ,
    MBR_DSBLTY_COV_TERM_DT_SK      CHAR(10)            NOT NULL     ,
    MBR_DSBLTY_IN                  CHAR(1)             NOT NULL     ,
    MBR_ETHNIC_CD                  VARCHAR(20)         NOT NULL     ,
    MBR_ETHNIC_NM                  VARCHAR(255)                     ,
    MBR_FEP_LOCAL_CNTR_IN          CHAR(1)             NOT NULL     ,
    MBR_FIRST_NM                   VARCHAR(35)                      ,
    MBR_MIDINIT                    CHAR(1)                          ,
    MBR_LAST_NM                    VARCHAR(75)                      ,
    MBR_FULL_NM                    VARCHAR(75)                      ,
    MBR_GNDR_CD                    VARCHAR(20)         NOT NULL     ,
    MBR_GNDR_NM                    VARCHAR(255)                     ,
    MBR_ID                         VARCHAR(20)         NOT NULL     ,
    MBR_INDV_BE_KEY                DECIMAL(11)         NOT NULL     ,
    MBR_LANG_CD                    VARCHAR(20)         NOT NULL     ,
    MBR_LANG_NM                    VARCHAR(255)        NOT NULL     ,
    MBR_MCAID_NO                   VARCHAR(20)                      ,
    MBR_MCARE_NO                   VARCHAR(20)                      ,
    MBR_MO_ST_MCAID_CNTY_CD        VARCHAR(20)         NOT NULL     ,
    MBR_MO_ST_MCAID_CNTY_NM        VARCHAR(255)                     ,
    MBR_OPTRN_VRSN_INDV_BE_KEY_TX  VARCHAR(20)         NOT NULL     ,
    MBR_PREX_COND_EFF_DT_SK        CHAR(10)            NOT NULL     ,
    MBR_PREX_COND_MO_QTY           SMALLINT            NOT NULL     ,
    MBR_RELSHP_CD                  VARCHAR(20)         NOT NULL     ,
    MBR_RELSHP_NM                  VARCHAR(255)                     ,
    MBR_SCRD_IN                    CHAR(1)             NOT NULL     ,
    MBR_SSN                        VARCHAR(20)         NOT NULL     ,
    MBR_STDNT_COV_EFF_DT_SK        CHAR(10)            NOT NULL     ,
    MBR_STDNT_COV_TERM_DT_SK       CHAR(10)            NOT NULL     ,
    MBR_STDNT_IN                   CHAR(1)             NOT NULL     ,
    MBR_SFX_NO                     VARCHAR(20)                      ,
    MBR_TERM_DT_SK                 CHAR(10)            NOT NULL     ,
    MBR_UNIQ_KEY_ORIG_EFF_DT_SK    CHAR(10)            NOT NULL     ,
    MBR_HOME_ADDR_LN_1             VARCHAR(40)                      ,
    MBR_HOME_ADDR_LN_2             VARCHAR(40)                      ,
    MBR_HOME_ADDR_LN_3             VARCHAR(40)                      ,
    MBR_HOME_ADDR_CITY_NM          VARCHAR(35)                      ,
    MBR_HOME_ADDR_ST_CD            VARCHAR(20)         NOT NULL     ,
    MBR_HOME_ADDR_ST_NM            VARCHAR(255)                     ,
    MBR_HOME_ADDR_ZIP_CD_5         CHAR(5)                          ,
    MBR_HOME_ADDR_ZIP_CD_4         CHAR(4)                          ,
    MBR_HOME_ADDR_CNTY_NM          VARCHAR(35)                      ,
    MBR_HOME_ADDR_PHN_NO           VARCHAR(20)                      ,
    MBR_HOME_ADDR_PHN_NO_EXT       CHAR(5)                          ,
    MBR_HOME_ADDR_FAX_NO           VARCHAR(20)                      ,
    MBR_HOME_ADDR_FAX_NO_EXT       CHAR(5)                          ,
    MBR_HOME_ADDR_EMAIL_ADDR_TX    VARCHAR(70)                      ,
    MBR_MAIL_ADDR_CONF_COMM_IN     CHAR(1)             NOT NULL     ,
    MBR_MAIL_ADDR_LN_1             VARCHAR(40)                      ,
    MBR_MAIL_ADDR_LN_2             VARCHAR(40)                      ,
    MBR_MAIL_ADDR_LN_3             VARCHAR(40)                      ,
    MBR_MAIL_ADDR_CITY_NM          VARCHAR(35)                      ,
    MBR_MAIL_ADDR_ST_CD            VARCHAR(20)         NOT NULL     ,
    MBR_MAIL_ADDR_ST_NM            VARCHAR(255)                     ,
    MBR_MAIL_ADDR_ZIP_CD_5         CHAR(5)                          ,
    MBR_MAIL_ADDR_ZIP_CD_4         CHAR(4)                          ,
    MBR_MAIL_ADDR_CNTY_NM          VARCHAR(35)                      ,
    MBR_MAIL_ADDR_PHN_NO           VARCHAR(20)                      ,
    MBR_MAIL_ADDR_PHN_NO_EXT       CHAR(5)                          ,
    MBR_MAIL_ADDR_FAX_NO           VARCHAR(20)                      ,
    MBR_MAIL_ADDR_FAX_NO_EXT       CHAR(5)                          ,
    MBR_MAIL_ADDR_EMAIL_ADDR_TX    VARCHAR(70)                      ,
    MBR_VAL_BASED_INCNTV_PGM_IN    CHAR(1)             NOT NULL     ,
    SUBGRP_ID                      VARCHAR(20)         NOT NULL     ,
    SUBGRP_NM                      VARCHAR(75)         NOT NULL     ,
    SUB_ALPHA_PFX_CD               VARCHAR(20)         NOT NULL     ,
    SUB_CNTGS_CNTY_CD              VARCHAR(20)         NOT NULL     ,
    SUB_CNTR_ST_CD                 VARCHAR(20)         NOT NULL     ,
    SUB_FIRST_NM                   VARCHAR(35)                      ,
    SUB_MIDINIT                    CHAR(1)                          ,
    SUB_LAST_NM                    VARCHAR(75)                      ,
    SUB_FULL_NM                    VARCHAR(75)                      ,
    SUB_HIRE_DT_SK                 CHAR(10)            NOT NULL     ,
    SUB_HIST_PRCS_DT_SK            CHAR(10)            NOT NULL     ,
    SUB_ID                         VARCHAR(20)         NOT NULL     ,
    SUB_IN_AREA_IN                 CHAR(1)             NOT NULL     ,
    SUB_IN                         CHAR(1)             NOT NULL     ,
    SUB_INDV_BE_KEY                DECIMAL(11)         NOT NULL     ,
    SUB_MKTNG_METRO_RURAL_CD       VARCHAR(20)         NOT NULL     ,
    SUB_RESDNC_CRS_PLN_CD          VARCHAR(20)         NOT NULL     ,
    SUB_RESDNC_CRS_PLN_NM          VARCHAR(255)                     ,
    SUB_RESDNC_SHIELD_PLN_CD       VARCHAR(20)         NOT NULL     ,
    SUB_RESDNC_SHIELD_PLN_NM       VARCHAR(255)                     ,
    SUB_RETR_DT_SK                 CHAR(10)            NOT NULL     ,
    SUB_SCRD_IN                    CHAR(1)             NOT NULL     ,
    SUB_SSN                        VARCHAR(20)         NOT NULL     ,
    SUB_UNIQ_KEY                   INTEGER             NOT NULL     ,
    SUB_UNIQ_KEY_ORIG_EFF_DT_SK    CHAR(10)            NOT NULL     ,
    WEB_PHYS_BNF_IN                CHAR(1)             NOT NULL     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    MBR_ETHNIC_CD_SK               INTEGER             NOT NULL     ,
    MBR_GNDR_CD_SK                 INTEGER             NOT NULL     ,
    MBR_HOME_ADDR_ST_CD_SK         INTEGER             NOT NULL     ,
    MBR_LANG_CD_SK                 INTEGER             NOT NULL     ,
    MBR_MAIL_ADDR_ST_CD_SK         INTEGER             NOT NULL     ,
    MBR_MO_ST_MCAID_CNTY_CD_SK     INTEGER             NOT NULL     ,
    MBR_RELSHP_CD_SK               INTEGER             NOT NULL     ,
    MBR_WORK_PHN_NO                VARCHAR(20)                      ,
    MBR_WORK_PHN_NO_EXT            CHAR(5)                          ,
    MBR_CELL_PHN_NO                VARCHAR(20)         NOT NULL  With Default ' '   ,
    HOST_MBR_IN                    CHAR(1)             NOT NULL  With Default 'N'   ,
    SPIRA_BNF_ID                   VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    MBR_HOME_ADDR_LAT_TX           DOUBLE                           ,
    MBR_HOME_ADDR_LONG_TX          DOUBLE                           );  """)
    print("==> created Table: MBR_D")

    cur.execute("""Create Or Replace Table DBO.MBR_ORIG_CT_F (
    MBR_ORIG_CT_SK                 INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    MBR_UNIQ_KEY                   INTEGER             NOT NULL     ,
    PROD_ID                        VARCHAR(20)         NOT NULL     ,
    ACTVTY_YR_MO_SK                CHAR(6)             NOT NULL     ,
    MBR_ENR_EFF_DT_SK              CHAR(10)            NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    CLS_SK                         INTEGER             NOT NULL     ,
    CLS_PLN_SK                     INTEGER             NOT NULL     ,
    GRP_SK                         INTEGER             NOT NULL     ,
    FNCL_LOB_SK                    INTEGER             NOT NULL     ,
    MBR_SK                         INTEGER             NOT NULL     ,
    ORIG_EXPRNC_CAT_SK             INTEGER             NOT NULL     ,
    ORIG_FNCL_LOB_SK               INTEGER             NOT NULL     ,
    PROV_SK                        INTEGER             NOT NULL     ,
    PROD_SK                        INTEGER             NOT NULL     ,
    SUBGRP_SK                      INTEGER             NOT NULL     ,
    MCARE_OPT_CD                   VARCHAR(20)         NOT NULL     ,
    PROD_BILL_CMPNT_COV_CAT_CD     VARCHAR(20)         NOT NULL     ,
    SUB_CALC_FMLY_DNTL_CNTR_CD     VARCHAR(20)         NOT NULL     ,
    SUB_CALC_FMLY_DNTL_CNTR_NM     VARCHAR(255)        NOT NULL     ,
    SUB_CALC_FMLY_MED_CNTR_CD      VARCHAR(20)         NOT NULL     ,
    SUB_CALC_FMLY_MED_CNTR_NM      VARCHAR(255)                     ,
    MCARE_DSBLD_IN                 CHAR(1)             NOT NULL     ,
    MCARE_RISK_ESRD_IN             CHAR(1)             NOT NULL     ,
    MCARE_RISK_HSPC_IN             CHAR(1)             NOT NULL     ,
    MCARE_RISK_INSTUT_IN           CHAR(1)             NOT NULL     ,
    MCARE_RISK_MCAID_IN            CHAR(1)             NOT NULL     ,
    MBR_MCARE_ELIG_IN              CHAR(1)             NOT NULL     ,
    ACTVTY_YR_MO_LAST_DT_OF_MO     CHAR(10)            NOT NULL     ,
    MBR_ENR_TERM_DT_SK             CHAR(10)            NOT NULL     ,
    CNTR_CT                        DECIMAL(6,2)        NOT NULL     ,
    CNTR_PRSN_CT                   DECIMAL(3)          NOT NULL     ,
    DPNDT_CT                       DECIMAL(6,2)        NOT NULL     ,
    DPNDT_PRSN_CT                  DECIMAL(3)          NOT NULL     ,
    MBR_AGE_AT_ACTVTY_YR_MO        DECIMAL(3)          NOT NULL     ,
    MBR_CT                         DECIMAL(6,2)        NOT NULL     ,
    MBR_PRSN_CT                    DECIMAL(3)          NOT NULL     ,
    SPOUSE_CT                      DECIMAL(6,2)        NOT NULL     ,
    SPOUSE_PRSN_CT                 DECIMAL(3)          NOT NULL     ,
    CLS_ID                         VARCHAR(20)         NOT NULL     ,
    CLS_PLN_ID                     VARCHAR(20)         NOT NULL     ,
    GRP_ID                         VARCHAR(20)         NOT NULL     ,
    GRP_UNIQ_KEY                   INTEGER             NOT NULL     ,
    MBR_SFX_NO                     VARCHAR(20)                      ,
    ORIG_EXPRNC_CAT_CD             VARCHAR(20)         NOT NULL     ,
    ORIG_FNCL_LOB_CD               VARCHAR(20)         NOT NULL     ,
    ORIG_MBR_HOME_ADDR_ZIP_CD_5    CHAR(5)                          ,
    ORIG_MBR_HOME_ADDR_ZIP_CD_4    CHAR(4)                          ,
    PCP_PROV_ID                    VARCHAR(20)         NOT NULL     ,
    PROD_SH_NM                     VARCHAR(20)         NOT NULL     ,
    SUBGRP_ID                      VARCHAR(20)         NOT NULL     ,
    SUB_UNIQ_KEY                   INTEGER             NOT NULL     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    PROD_BILL_CMPNT_COV_TYP_CD     VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    PROD_BILL_CMPNT_COV_TYP_NM     VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    PROD_BILL_CMPNT_COV_TYP_CD_SK  INTEGER             NOT NULL  With Default 1   ,
    QHP_ID                         VARCHAR(40)         NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_CD                VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_NM                VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_CD_SK             INTEGER             NOT NULL  With Default 1   ,
    QHP_ENR_TYP_CD                 VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_ENR_TYP_NM                 VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_ENR_TYP_CD_SK              INTEGER             NOT NULL  With Default 1   ,
    QHP_EXCH_CHAN_CD               VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_EXCH_CHAN_NM               VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_EXCH_CHAN_CD_SK            INTEGER             NOT NULL  With Default 1   ,
    QHP_EXCH_TYP_CD                VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_EXCH_TYP_NM                VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_EXCH_TYP_CD_SK             INTEGER             NOT NULL  With Default 1   ,
    QHP_METAL_LVL_CD               VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_METAL_LVL_NM               VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_METAL_LVL_CD_SK            INTEGER             NOT NULL  With Default 1   ,
    QHP_APTC_IN                    CHAR(1)             NOT NULL  With Default 'N'   ,
    QHP_CSR_IN                     CHAR(1)             NOT NULL  With Default 'N'   ,
    ACTURL_VAL_NO                  DECIMAL(6,4)                     ,
    MLR_GRP_SIZE_CD                VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    SPIRA_BNF_ID                   VARCHAR(20)                   With Default 'NA'   ); """)
    print("==> created Table: MBR_ORIG_CT_F")
    
    cur.execute("""Create Or Replace Table DBO.MBR_RCST_VNDR_CT_F (
    MBR_RCST_VNDR_SK               INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    ACTVTY_YR_MO_SK                CHAR(6)             NOT NULL     ,
    FNCL_LOB_CD                    VARCHAR(20)         NOT NULL     ,
    PROD_SH_NM                     VARCHAR(20)         NOT NULL     ,
    VNDR_CD                        VARCHAR(20)         NOT NULL     ,
    PROD_BILL_CMPNT_COV_CAT_CD     VARCHAR(20)         NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    FNCL_LOB_SK                    INTEGER             NOT NULL     ,
    PROD_SH_NM_SK                  INTEGER             NOT NULL     ,
    CNTR_CT                        DECIMAL(7,1)        NOT NULL     ,
    MBR_CT                         DECIMAL(7,1)        NOT NULL     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    SPIRA_BNF_ID                   VARCHAR(20)         NOT NULL  With Default 'NA'   ); """)
    print("==> created Table: MBR_RCST_VNDR_CT_F")

    cur.execute("""Create Or Replace Table DBO.PROD_D (
    PROD_SK                        INTEGER             NOT NULL     ,
    SRC_SYS_CD                     VARCHAR(20)         NOT NULL     ,
    PROD_ID                        VARCHAR(20)         NOT NULL     ,
    CRT_RUN_CYC_EXCTN_DT_SK        CHAR(10)            NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK  CHAR(10)            NOT NULL     ,
    EXPRNC_CAT_CD                  VARCHAR(20)         NOT NULL     ,
    EXPRNC_CAT_DESC                VARCHAR(70)                      ,
    FNCL_LOB_CD                    VARCHAR(20)         NOT NULL     ,
    FNCL_LOB_DESC                  VARCHAR(70)                      ,
    PROD_ABBR                      VARCHAR(20)                      ,
    PROD_DNTL_BILL_PFX_CD          VARCHAR(20)         NOT NULL     ,
    PROD_DNTL_BILL_PFX_NM          VARCHAR(255)                     ,
    PROD_DNTL_LATE_WAIT_IN         CHAR(1)             NOT NULL     ,
    PROD_DESC                      VARCHAR(70)                      ,
    PROD_DRUG_COV_IN               CHAR(1)             NOT NULL     ,
    PROD_EFF_DT_SK                 CHAR(10)            NOT NULL     ,
    PROD_FCTS_CONV_RATE_PFX        VARCHAR(20)                      ,
    PROD_HLTH_COV_IN               CHAR(1)             NOT NULL     ,
    PROD_LOB_NO                    VARCHAR(20)                      ,
    PROD_LOB_CD                    VARCHAR(20)         NOT NULL     ,
    PROD_LOB_NM                    VARCHAR(255)                     ,
    PROD_PCKG_CD                   VARCHAR(20)         NOT NULL     ,
    PROD_PCKG_DESC                 VARCHAR(255)        NOT NULL     ,
    PROD_MNL_PRCS_IN               CHAR(1)             NOT NULL     ,
    PROD_MNTL_HLTH_COV_IN          CHAR(1)             NOT NULL     ,
    PROD_RATE_TYP_CD               VARCHAR(20)         NOT NULL     ,
    PROD_RATE_TYP_NM               VARCHAR(255)                     ,
    PROD_ST_CD                     VARCHAR(20)         NOT NULL     ,
    PROD_ST_NM                     VARCHAR(255)                     ,
    PROD_SUBPROD_CD                VARCHAR(20)         NOT NULL     ,
    PROD_SUBPROD_NM                VARCHAR(255)                     ,
    PROD_TERM_DT_SK                CHAR(10)            NOT NULL     ,
    PROD_VSN_COV_IN                CHAR(1)             NOT NULL     ,
    PROD_SH_NM                     VARCHAR(20)         NOT NULL     ,
    PROD_SH_NM_CAT_CD              VARCHAR(20)         NOT NULL     ,
    PROD_SH_NM_CAT_NM              VARCHAR(255)                     ,
    PROD_SH_NM_DLVRY_METH_CD       VARCHAR(20)         NOT NULL     ,
    PROD_SH_NM_DLVRY_METH_NM       VARCHAR(255)                     ,
    PROD_SH_NM_DESC                VARCHAR(70)                      ,
    PROD_SHNM_MCARE_SUPLMT_COV_IN  CHAR(1)             NOT NULL     ,
    CRT_RUN_CYC_EXCTN_SK           INTEGER             NOT NULL     ,
    LAST_UPDT_RUN_CYC_EXCTN_SK     INTEGER             NOT NULL     ,
    EXPRNC_CAT_SK                  INTEGER             NOT NULL     ,
    FNCL_LOB_SK                    INTEGER             NOT NULL     ,
    PROD_DNTL_BILL_PFX_CD_SK       INTEGER             NOT NULL     ,
    PROD_LOB_CD_SK                 INTEGER             NOT NULL     ,
    PROD_PCKG_CD_SK                INTEGER             NOT NULL     ,
    PROD_RATE_TYP_CD_SK            INTEGER             NOT NULL     ,
    PROD_SH_NM_SK                  INTEGER             NOT NULL     ,
    PROD_SH_NM_CAT_CD_SK           INTEGER             NOT NULL     ,
    PROD_SH_NM_DLVRY_METH_CD_SK    INTEGER             NOT NULL     ,
    PROD_ST_CD_SK                  INTEGER             NOT NULL     ,
    PROD_SUBPROD_CD_SK             INTEGER             NOT NULL     ,
    QHP_SK                         INTEGER                       With Default 1   ,
    QHP_ID                         VARCHAR(40)         NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_CD                VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_NM                VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_CSR_VRNT_CD_SK             INTEGER             NOT NULL  With Default 1   ,
    QHP_ENR_TYP_CD                 VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_ENR_TYP_NM                 VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_ENR_TYP_CD_SK              INTEGER             NOT NULL  With Default 1   ,
    QHP_EXCH_CHAN_CD               VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_EXCH_CHAN_NM               VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_EXCH_CHAN_CD_SK            INTEGER             NOT NULL  With Default 1   ,
    QHP_EXCH_TYP_CD                VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_EXCH_TYP_NM                VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_EXCH_TYP_CD_SK             INTEGER             NOT NULL  With Default 1   ,
    QHP_METAL_LVL_CD               VARCHAR(20)         NOT NULL  With Default 'NA'   ,
    QHP_METAL_LVL_NM               VARCHAR(255)        NOT NULL  With Default 'NA'   ,
    QHP_METAL_LVL_CD_SK            INTEGER             NOT NULL  With Default 1   ,
    ACTURL_VAL_NO                  DECIMAL(6,4)                     ,
    SPIRA_BNF_ID                   VARCHAR(20)         NOT NULL  With Default 'NA'   ); """)
    print("==> created Table: PROD_D")

    cur.execute("""Create Or Replace Table DBO.YR_MO_D (
    YR_MO_SK                       CHAR(6)             NOT NULL     ,
    MO_FULL_NM                     VARCHAR(20)         NOT NULL     ,
    MO_NO                          DECIMAL(2)          NOT NULL     ,
    MO_NO_TX                       CHAR(2)             NOT NULL     ,
    MO_SH_NM                       VARCHAR(20)         NOT NULL     ,
    FIRST_DT_OF_MO                 CHAR(10)            NOT NULL     ,
    LAST_DT_OF_MO                  CHAR(10)            NOT NULL     ,
    TWO_DGT_YR_NO                  INTEGER             NOT NULL     ,
    TWO_DGT_YR_TX                  VARCHAR(20)         NOT NULL     ,
    YR_MO_FMT_TX                   CHAR(7)             NOT NULL     ,
    YR_NO                          INTEGER             NOT NULL     ,
    YR_QTR_SK                      CHAR(6)             NOT NULL     ,
    YR_SK                          CHAR(4)             NOT NULL     ,
    FIRST_DT_OF_MO_DT              DATE                             ,
    LAST_DT_OF_MO_DT               DATE                             );""")
    print("==> created Table: YR_MO_D")

    cur.close()

try:
    sfConnect = snowflake.connector.connect(
                user=sfUser,
                password=sfPswd,
                account=sfAccount,
                role=sfRole,
                warehouse=sfWarehouse
                ) 

    create_db(sfConnect)
    create_schema(sfConnect)
    #create_warehouse(sfConnect)
    create_tables(sfConnect)

finally:
    if 'sfConnect' in locals():
        sfConnect.close()
        print("==> connection closed")

