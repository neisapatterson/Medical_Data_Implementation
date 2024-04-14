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

# meta_path = 'allfiles.yaml'
li = []

# for csv_path in csvlist:
#     df = pd.read_csv(csv_path)
#     print (df.info())
#     li.append(df)

# for (csv_path, meta_path) in zip(csvlist, metalist):
#     df = pd.read_csv(csv_path)
#     li.append(df)
#     # print(pums)
#     print (df.info())

# frame = pd.concat(li, axis=0, ignore_index=True)
# reader = snsql.from_df(frame, privacy = privacy, metadata=meta_path)

df = pd.read_csv(csv_path4)
print(df.info())
reader = snsql.from_df(df, privacy = privacy, metadata=meta_path4)

result = reader.execute(' SELECT I.PUMS.Sex, COUNT(*) AS Death_Count FROM I.PUMS WHERE I.PUMS.HAZARD_SEVERITY_CODE_E = \'DEATH\' GROUP BY I.PUMS.Sex')
result = reader.execute(' SELECT COUNT(*) AS Death_Count FROM I.PUMS WHERE I.PUMS.HAZARD_SEVERITY_CODE_E = \'DEATH\'')
    # SELECT  \
    #     ID.DEVICE_ID,\
    #     ID.TRADE_NAME, \
    #     ID.RISK_CLASSIFICATION, \
    #     I.Sex, \
    #     COUNT(*) AS Death_Count \
    # FROM \
    #     I.PUMS as I\
    # JOIN \
    #     ID ON I.INCIDENT_ID = ID.PUMS.INCIDENT_ID \
    # WHERE \
    #     I.HAZARD_SEVERITY_CODE_E = \'DEATH\' \
    # GROUP BY \
    #     ID.DEVICE_ID, \
    #     ID.TRADE_NAME, \
    #     I.Sex;')
print(result)
