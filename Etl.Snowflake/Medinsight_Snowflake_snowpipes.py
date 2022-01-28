import os
import sys
import snowflake.connector

if len(sys.argv) != 8:
    print("==> Incorrect # of parms. Usage: ",__file__," SnowflakeAccount Username Password Role Warehouse Database Schema")
    sys.exit(1)

sfAccount = sys.argv[1]
sfUser = sys.argv[2]
#sfPswd =''
sfPswd = sys.argv[3]

if sfPswd == '':
    import getpass
    sfPswd = getpass.getpass('Password:')

sfRole = sys.argv[4]
sfWarehouse = sys.argv[5]
sfDatabase = sys.argv[6]
sfSchema = sys.argv[7]

'''
# Create External Stage
def create_stage(sfConnect):
    cur = sfConnect.cursor()
    cur.execute("""CREATE OR REPLACE STAGE MEDINSIGHT_ENTDL_STAGE
url = 'azure://entdluscnonprod.blob.core.windows.net/raw/MedInsight/'
credentials = (azure_sas_token= '?sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2020-05-08T00:01:16Z&st=2020-05-07T04:01:16Z&spr=https&sig=Tjy2qvmwmRFECyNfdNJ8MrD5xmnBj46CdAAN%2FXijLzs%3D'
              );""")
    print("==> created External Stage: MEDINSIGHT_ENTDL_STAGE")
    cur.close() '''

# Create Snowpipes
def create_snowpipes(sfConnect):
    cur = sfConnect.cursor()

    cur.execute("""CREATE OR REPLACE pipe DIM_ADM_SRC_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_ADM_SRC
  from @MEDINSIGHT_ENTDL_STAGE/DIM_ADM_SRC
  file_format = (format_name = TDF);""")
    print("==> Created Snowpipe: DIM_ADM_SRC_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_ADM_TYPE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_ADM_TYPE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_ADM_TYPE
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_ADM_TYPE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_AVOIDABLE_ED_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_AVOIDABLE_ED
  from @MEDINSIGHT_ENTDL_STAGE/DIM_AVOIDABLE_ED
  file_format = (format_name = TDF);

""")
    print("==> Created Snowpipe: DIM_AVOIDABLE_ED_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CAPITATION_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CAPITATION
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CAPITATION
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_CAPITATION_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CCHG_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CCHG
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CCHG
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_CCHG_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CCHG_FLAGS_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CCHG_FLAGS
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CCHG_FLAGS
  file_format = (format_name = TDF);
  """)
    print("==> Created Snowpipe: DIM_CCHG_FLAGS_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CPT_MOD_01_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CPT_MOD_01
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CPT_MOD_01
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_CPT_MOD_01_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CPT_MOD_02_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CPT_MOD_02
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CPT_MOD_02
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_CPT_MOD_02_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_CPT_PROC_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_CPT_PROC
  from @MEDINSIGHT_ENTDL_STAGE/DIM_CPT_PROC
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_CPT_PROC_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_ADMIT_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_ADMIT
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_ADMIT
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_DATE_ADMIT_PIPE")
    
    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_DISCHARGE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_DISCHARGE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_DISCHARGE
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_DATE_DISCHARGE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_EBM_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_EBM
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_EBM
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_DATE_EBM_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_EPISODE_END_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_EPISODE_END
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_EPISODE_END
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_DATE_EPISODE_END_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_INCURRED_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_INCURRED
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_INCURRED
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_DATE_INCURRED_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DATE_PAID_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DATE_PAID
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DATE_PAID
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_DATE_PAID_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DIAGNOSIS_ICD_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DIAGNOSIS_ICD
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DIAGNOSIS_ICD
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_DIAGNOSIS_ICD_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DIS_STAT_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DIS_STAT
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DIS_STAT
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_DIS_STAT_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_DRG_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_DRG
  from @MEDINSIGHT_ENTDL_STAGE/DIM_DRG
  file_format = (format_name = TDF);
""")
    print("==> Created Snowpipe: DIM_DRG_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_EBM_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_EBM
  from @MEDINSIGHT_ENTDL_STAGE/DIM_EBM
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_EBM_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_ETG_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_ETG
  from @MEDINSIGHT_ENTDL_STAGE/DIM_ETG
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_ETG_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_GROUP_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_GROUP
  from @MEDINSIGHT_ENTDL_STAGE/DIM_GROUP
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_GROUP_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_GRVU_INDICATOR_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_GRVU_INDICATOR
  from @MEDINSIGHT_ENTDL_STAGE/DIM_GRVU_INDICATOR
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_GRVU_INDICATOR_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_HCC_MODEL_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_HCC_MODEL
  from @MEDINSIGHT_ENTDL_STAGE/DIM_HCC_MODEL
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_HCC_MODEL_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_HCG_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_HCG
  from @MEDINSIGHT_ENTDL_STAGE/DIM_HCG
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_HCG_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MARA_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MARA
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MARA
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_MARA_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MED_HOME_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MED_HOME
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MED_HOME
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_MED_HOME_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEDICARE_INDICATOR_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEDICARE_INDICATOR
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEDICARE_INDICATOR
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_MEDICARE_INDICATOR_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MEMBER_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_AGE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_AGE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_AGE
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_AGE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_GENDER_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_GENDER
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_GENDER
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_GENDER_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_GEO_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_GEO
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_GEO
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_GEO_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_RELATION_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_RELATION
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_RELATION
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_RELATION_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_STAT_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_STAT
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_STAT
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_STAT_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MEMBER_INFO_TIER_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MEMBER_INFO_TIER
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MEMBER_INFO_TIER
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MEMBER_INFO_TIER_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_MM_UDF_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_MM_UDF
  from @MEDINSIGHT_ENTDL_STAGE/DIM_MM_UDF
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_MM_UDF_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PAYER_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PAYER
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PAYER
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PAYER_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PBP_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PBP
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PBP
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PBP_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROCEDURE_ICD_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROCEDURE_ICD
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROCEDURE_ICD
  file_format = (format_name = TDF);
  
 """)
    print("==> Created Snowpipe: DIM_PROCEDURE_ICD_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROVIDER_BILLING_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_BILLING
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_BILLING
  file_format = (format_name = TDF);
 
 """)
    print("==> Created Snowpipe: DIM_PROVIDER_BILLING_PIPE")

    cur.execute(""" CREATE OR REPLACE pipe DIM_PROVIDER_CAP_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_CAP
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_CAP
  file_format = (format_name = TDF);
 """)
    print("==> Created Snowpipe: DIM_PROVIDER_CAP_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROVIDER_ETG_ATTRIB_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_ETG_ATTRIB
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_ETG_ATTRIB
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PROVIDER_ETG_ATTRIB_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROVIDER_PCP_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_PCP
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_PCP
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PROVIDER_PCP_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROVIDER_PCP_ATTRIB_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_PCP_ATTRIB
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_PCP_ATTRIB
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PROVIDER_PCP_ATTRIB_PIPE")

    cur.execute(""" CREATE OR REPLACE pipe DIM_PROVIDER_REFERRING_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_REFERRING
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_REFERRING
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PROVIDER_REFERRING_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_PROVIDER_SERVICE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_PROVIDER_SERVICE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_PROVIDER_SERVICE
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_PROVIDER_SERVICE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_REV_CODE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_REV_CODE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_REV_CODE
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_REV_CODE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_RVU_FINAL_STEP_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_RVU_FINAL_STEP
  from @MEDINSIGHT_ENTDL_STAGE/DIM_RVU_FINAL_STEP
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_RVU_FINAL_STEP_PIPE")
     
    cur.execute("""CREATE OR REPLACE pipe DIM_RX_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_RX
  from @MEDINSIGHT_ENTDL_STAGE/DIM_RX
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_RX_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_SCDGC_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_SCDGC
  from @MEDINSIGHT_ENTDL_STAGE/DIM_SCDGC
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_SCDGC_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_UB_BILL_TYPE_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_UB_BILL_TYPE
  from @MEDINSIGHT_ENTDL_STAGE/DIM_UB_BILL_TYPE
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_UB_BILL_TYPE_PIPE")

    cur.execute("""CREATE OR REPLACE pipe DIM_WASTE_LINE_NEW_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_WASTE_LINE_NEW
  from @MEDINSIGHT_ENTDL_STAGE/DIM_WASTE_LINE_NEW
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_WASTE_LINE_NEW_PIPE")
    
    cur.execute("""CREATE OR REPLACE pipe DIM_WASTE_NEW_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into DIM_WASTE_NEW
  from @MEDINSIGHT_ENTDL_STAGE/DIM_WASTE_NEW
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: DIM_WASTE_NEW_PIPE")
 
    cur.execute("""CREATE OR REPLACE pipe FACT_CAPITATION_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into FACT_CAPITATION
  from @MEDINSIGHT_ENTDL_STAGE/FACT_CAPITATION
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: FACT_CAPITATION_PIPE")

    cur.execute("""CREATE OR REPLACE pipe FACT_EBM_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into FACT_EBM
  from @MEDINSIGHT_ENTDL_STAGE/FACT_EBM
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: FACT_EBM_PIPE")

    cur.execute("""CREATE OR REPLACE pipe FACT_MEMBER_MONTHS_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into FACT_MEMBER_MONTHS
  from @MEDINSIGHT_ENTDL_STAGE/FACT_MEMBER_MONTHS
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: FACT_MEMBER_MONTHS_PIPE")

    cur.execute("""CREATE OR REPLACE pipe FACT_SERVICES_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into FACT_SERVICES
  from @MEDINSIGHT_ENTDL_STAGE/FACT_SERVICES
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: FACT_SERVICES_PIPE")

    cur.execute("""CREATE OR REPLACE pipe FACT_WASTE_EVENT_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into FACT_WASTE_EVENT
  from @MEDINSIGHT_ENTDL_STAGE/FACT_WASTE_EVENT
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: FACT_WASTE_EVENT_PIPE")

    cur.execute(""" CREATE OR REPLACE pipe RPT_CONTINUOUS_STAY_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_CONTINUOUS_STAY
  from @MEDINSIGHT_ENTDL_STAGE/RPT_CONTINUOUS_STAY
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_CONTINUOUS_STAY_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD01_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD01
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD01
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD01_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD02_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD02
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD02
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD02_PIPE")

    cur.execute("""  CREATE OR REPLACE pipe RPT_UD03_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD03
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD03
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD03_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD04_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD04
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD04
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD04_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD05_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD05
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD05
  file_format = (format_name = TDF); 

 """)
    print("==> Created Snowpipe: RPT_UD05_PIPE") 

    cur.execute("""CREATE OR REPLACE pipe RPT_UD06_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD06
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD06
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD06_PIPE") 

    cur.execute("""CREATE OR REPLACE pipe RPT_UD07_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD07
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD07
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD07_PIPE")

    cur.execute(""" CREATE OR REPLACE pipe RPT_UD08_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD08
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD08
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD08_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD09_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD09
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD09
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD09_PIPE")

    cur.execute("""CREATE OR REPLACE pipe RPT_UD10_PIPE
  auto_ingest = true
  integration = 'ADLS_NOTIFICATION_INTEGRATION'
  as
  copy into RPT_UD10
  from @MEDINSIGHT_ENTDL_STAGE/RPT_UD10
  file_format = (format_name = TDF);

 """)
    print("==> Created Snowpipe: RPT_UD10_PIPE")

    cur.close()

try:
    sfConnect = snowflake.connector.connect(
                user=sfUser,
                password=sfPswd,
                account=sfAccount,
                role=sfRole,
                warehouse=sfWarehouse,
                database=sfDatabase,
                schema=sfSchema
                ) 

    #create_stage(sfConnect)
    create_snowpipes(sfConnect)

finally:
    if 'sfConnect' in locals():
        sfConnect.close()
        print("==> connection closed")

