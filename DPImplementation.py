import snsql
from snsql import Privacy
import pandas as pd
privacy = Privacy (epsilon = 1.0, delta=(1/(514034*716)))

csv_path1 = 'CSV_Versions/COPY_INCIDENT_COMPANY.csv'
csv_path2 = 'CSV_Versions/COPY_INCIDENT_DEVICE.csv'
csv_path3 = 'COPY_INCIDENT_PATIENT_DEVICE_CODE.csv'
csv_path4 = 'CSV_Versions/COPY_INCIDENT.csv'
csv_path5 = 'CSV_Versions/COPY_PATIENT_DEVICE_CODE_TABLE.csv'
csv_path6 = 'CSV_Versions/COPY_PREF_NAME_CODE_TABLE.csv'
# csv_path = 'CSV_Versions/Test.csv'
meta_path = 'metaIC.yaml'

df = pd.concat(map(pd.read_csv, ['csv_path1.csv', 'csv_path2.csv','csv_path3.csv_path4', 'csv_path5', 'csv_path6']))
pums = pd.read_csv(csv_path)
# print(pums)
print (pums.info())
reader = snsql.from_df(pums, privacy = privacy, metadata=meta_path)

result = reader.execute('SELECT INCIDENT_ID, AVG(COMPANY_ID) FROM PUMS.PUMS GROUP BY INCIDENT_ID')
print(result)
