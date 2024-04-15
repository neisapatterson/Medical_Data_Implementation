import snsql
from snsql import Privacy
import pandas as pd
from tabulate import tabulate
# privacy = Privacy (epsilon = 1.0, delta=(1/(514034*716)))
privacy = Privacy (epsilon = 100.0, delta=(1/(303032*550)))

csv_path1 = 'NewCSV/INCIDENT_COMPANY.csv'
csv_path2 = 'NewCSV/INCIDENT_DEVICE.csv'
csv_path3 = 'NewCSV/INCIDENT_PATIENT_DEVICE_CODE.csv'
csv_path4 = 'NewCSV/INCIDENT.csv'
csv_path5 = 'NewCSV/PATIENT_DEVICE_CODE_TABLE.csv'
csv_path6 = 'NewCSV/PREF_NAME_CODE_TABLE.csv'
csvlist   = [csv_path1, csv_path2, csv_path3, csv_path4, csv_path5, csv_path6]
# csv_path = 'CSV_Versions/Test.csv'
meta_path1 = 'metaIC.yaml'
meta_path2 = 'metaID.yaml'
meta_path3 = 'metaIPDC.yaml'
meta_path4 = 'metaI.yaml'
meta_path5 = 'metaPDCT.yaml'
meta_path6 = 'metaPNCT.yaml'
metalist   = [meta_path1, meta_path2, meta_path3, meta_path4, meta_path5, meta_path6]

meta_path = 'IJOINID.yaml'

ID   = pd.read_csv(csv_path2)
I    = pd.read_csv(csv_path4)
PNCT = pd.read_csv(csv_path6)

df = pd.merge(I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E', 'Sex']], ID[['INCIDENT_ID', 'TRADE_NAME', 'PREF_NAME_CODE']], on='INCIDENT_ID', how='inner')
df = pd.merge(df, PNCT[['PREF_NAME_CODE', 'PREF_DESC_E']], on='PREF_NAME_CODE', how='inner')
print(df.info())

reader = snsql.from_df(df, privacy = privacy, metadata=meta_path)

result = reader.execute('\
        SELECT PREF_DESC_E, TRADE_NAME, Sex, COUNT(INCIDENT_ID) AS Death_Count \
        FROM IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
            I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
        GROUP BY PREF_DESC_E, TRADE_NAME, Sex ORDER BY Death_Count DESC LIMIT 5')

# result = reader.execute('\
#         SELECT PREF_DESC_E, Sex, COUNT(INCIDENT_ID) AS Death_Count \
#         FROM IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
#             I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
#         GROUP BY PREF_DESC_E, Sex ORDER BY Death_Count DESC LIMIT 5')

print(tabulate(result, headers="firstrow"))



IC  = pd.read_csv(csv_path1)
df = pd.merge(I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E']], IC[['INCIDENT_ID', 'COMPANY_NAME']], on='INCIDENT_ID', how='inner')

reader = snsql.from_df(df, privacy = privacy, metadata='IJOINIC.yaml')

result = reader.execute('\
        SELECT COMPANY_NAME, COUNT(INCIDENT_ID) AS Death_Count \
        FROM IJOINIC.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
            I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
        GROUP BY COMPANY_NAME ORDER BY Death_Count DESC LIMIT 5')

print(tabulate(result, headers="firstrow"))


