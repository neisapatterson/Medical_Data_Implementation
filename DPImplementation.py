import snsql
from snsql import Privacy
import pandas as pd
privacy = Privacy (epsilon = 1.0, delta=(1/(514034*716)))

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

# df = pd.concat(map(pd.read_csv, ['csv_path1.csv', 'csv_path2.csv','csv_path3.csv_path4', 'csv_path5', 'csv_path6']))
for (csv_path, meta_path) in zip(csvlist, metalist):
    pums = pd.read_csv(csv_path)
    # print(pums)
    print (pums.info())
    reader = snsql.from_df(pums, privacy = privacy, metadata=meta_path)

    # result = reader.execute('SELECT INCIDENT_ID, AVG(COMPANY_ID) FROM IC.PUMS GROUP BY INCIDENT_ID')
    # print(result)
