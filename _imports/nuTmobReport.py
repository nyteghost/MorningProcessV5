import pandas as pd
import getpass
import time
from mpConfigs.dbConfig import dbConnect

startTime = time.time()

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT TMobile Subscriber_Report.csv"
relative_file_path = prefix + "\\" + localuser + suffix

print('Initializing column mapping.\n')

column_mapping = {
    'BAN': 'BAN',
    'Mobile Number': '"Mobile Number"',
    'Equipment Manufacturer': '"Equipment Manufacturer"',
    'Upgrade Eligible': '"Upgrade Eligible"',
    'Primary Plan': '"Primary Plan"',
    'Primary Plan SOC': '"Primary Plan SOC"',
    'SIM': 'SIM',
    'Upgrade Eligibility Date': '"Upgrade Eligibility Date"',
    'IMEI': 'IMEI',
    'Device Model Name': '"Device Model Name"',
    'Device User': '"Device User"',
    'DAC': 'DAC',
    'User Def 1': '"User Def 1"',
    'User Def 2': '"User Def 2"',
    'User Def 3': '"User Def 3"',
    'User Def 4': '"User Def 4"',
    'Status': 'Status',
    'Last Updated': '"Last Updated"'
}


tic = time.time()

csv = pd.read_csv(relative_file_path)
df = pd.DataFrame(csv, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(csv), None)

connect = dbConnect("gcaassetmgmt_2_0")
connect.df_to_sql(df, 'rep_tmobilereport')


toc = time.time()
print('Done in {:.4f} seconds'.format(toc - tic))
