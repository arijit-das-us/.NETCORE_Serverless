import os
import sys
import snowflake.connector

if len(sys.argv) != 8:
    print("==> Incorrect # of parms. Usage: ",__file__," SnowflakeAccount Username Password Role Warehouse Database Schema")
    sys.exit(1)

sfAccount = sys.argv[1]
sfUser = sys.argv[2]
sfPswd =''
#sfPswd = sys.argv[3]

if sfPswd == '':
    import getpass
    sfPswd = getpass.getpass('Password:')

sfRole = sys.argv[4]
sfWarehouse = sys.argv[5]
sfDatabase = sys.argv[6]
sfSchema = sys.argv[7]

DataFilesList = []
CTLFileList = []
TablesList = []

# Create List for Data Files, Control files and Tables by parsing through the Files list
def create_list(sfConnect):
    cur = sfConnect.cursor()
    cur.execute("select distinct metadata$filename from @"+sfDatabase+"_ENTDL_STAGE;")
    filenames = [i[0] for i in cur.fetchall()]

    for n in filenames:
        if n[-4:] == '.DAT':
            #k = n.split('/')[1]
            k=n.rsplit('/')[-1]
            DataFilesList.append(k)
            TablesList.append(k.rsplit('_',1)[0])
        if n[-4:] == '.CTL':
            #CTLFileList.append(n.split('/')[1])
            CTLFileList.append(n.rsplit('/')[-1])
    cur.close()
    
# Load the Data from Files into tables on Snowflake
def copy_into_tables(sfConnect):
    cur = sfConnect.cursor()
    for datafile in DataFilesList:
        TABLE_NM = datafile.rsplit('_',1)[0]
        cur.execute("COPY INTO "+TABLE_NM+" from @"+sfDatabase+"_ENTDL_STAGE/"+datafile+" file_format = (format_name = TDF);")
        print("==> Table",TABLE_NM, " loaded")
    cur.close()

# Retrieve Record Counts from Control files and Load CONTROL_FILE_CT on LOAD_BAL table
def control_file_ct(sfConnect):
    cur = sfConnect.cursor()
    for ctlfile in CTLFileList:
        cur.execute("select $1 from @"+sfDatabase+"_ENTDL_STAGE/"+ctlfile+" (file_format => TDF);")
        CTL_FILE_CT = cur.fetchone()[0]
        FILE_RCVD_DT = ctlfile.rsplit('_')[-1].split('.CTL')[0]
        if '_CTRL' in ctlfile:
            TABLE_NM = ctlfile.rsplit('_',2)[0]
        else:
            TABLE_NM = ctlfile.rsplit('_',1)[0]
        cur.execute("insert into "+sfDatabase+"_LOAD_BAL(TBL_NAME, FILE_RCVD_DT,CONTROL_FILE_CT) values ('"+TABLE_NM+"','"+FILE_RCVD_DT+"',"+CTL_FILE_CT+");")    
    cur.close()

# Count total records on Data Files and Load DATA_FILE_CT on LOAD_BAL table
def data_file_ct(sfConnect):
    cur = sfConnect.cursor()
    for datfile in DataFilesList:
        cur.execute("select count(*) from @"+sfDatabase+"_ENTDL_STAGE/"+datfile+" (file_format => TDF);")
        DAT_FILE_CT = str(cur.fetchone()[0])
        FILE_RCVD_DT = datfile.rsplit('_')[-1].split('.DAT')[0]
        TABLE_NM = datfile.rsplit('_',1)[0]
        cur.execute("merge into "+sfDatabase+"_LOAD_BAL AS LB using (select '"+TABLE_NM+"' as TBL, '"+FILE_RCVD_DT+"' as RCVD_DT, "+DAT_FILE_CT+""" as CT from dual) as B on LB.TBL_NAME = B.TBL and LB.FILE_RCVD_DT = B.RCVD_DT
  when matched then update set LB.DATA_FILE_CT = B.CT
  when not matched then insert (LB.TBL_NAME, LB.FILE_RCVD_DT, LB.DATA_FILE_CT) values (B.TBL, B.RCVD_DT, B.CT);""")
    cur.close()

# Count total records populated on SF tables and Load SF_TABLE_CT on LOAD_BAL table
def sf_tables_ct(sfConnect):
    cur = sfConnect.cursor()
    for tbl in DataFilesList:
        FILE_RCVD_DT = tbl.rsplit('_')[-1].split('.DAT')[0]
        TABLE_NM = tbl.rsplit('_',1)[0]
        cur.execute("select count(*) from "+TABLE_NM+";")
        TBL_CT = str(cur.fetchone()[0])
        cur.execute("merge into "+sfDatabase+"_LOAD_BAL AS LB using (select '"+TABLE_NM+"' as TBL, '"+FILE_RCVD_DT+"' as RCVD_DT, "+TBL_CT+""" as CT from dual) as B on LB.TBL_NAME = B.TBL and LB.FILE_RCVD_DT = B.RCVD_DT
  when matched then update set LB.SF_TABLE_CT = B.CT
  when not matched then insert (LB.TBL_NAME, LB.FILE_RCVD_DT, LB.SF_TABLE_CT) values (B.TBL, B.RCVD_DT, B.CT);""")
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

    create_list(sfConnect)
    copy_into_tables(sfConnect)
    control_file_ct(sfConnect)
    data_file_ct(sfConnect)
    sf_tables_ct(sfConnect)

finally:
    if 'sfConnect' in locals():
        sfConnect.close()
        print("==> connection closed")

 