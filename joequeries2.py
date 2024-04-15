import snsql
from snsql import Privacy
import pandas as pd
from tabulate import tabulate
privacy = Privacy (epsilon = 100.0, delta=(1/(303032*550)))


csv_path2 = 'INCIDENT_DEVICE.csv'

csv_path4 = 'INCIDENT.csv'

csv_path6 = 'PREF_NAME_CODE_TABLE.csv'
csvlist   = [csv_path2, csv_path4, csv_path6]

meta_path = 'IJOINID.yaml'

id_csv   = pd.read_csv(csv_path2)
inci_csv    = pd.read_csv(csv_path4)
pnct_csv = pd.read_csv(csv_path6)

df = pd.merge(inci_csv[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E', 'Sex']], id_csv[['INCIDENT_ID', 'TRADE_NAME', 'PREF_NAME_CODE']], on='INCIDENT_ID', how='inner')
df = pd.merge(df, pnct_csv[['PREF_NAME_CODE', 'PREF_DESC_E']], on='PREF_NAME_CODE', how='inner')
print(df.info())


# This is the 3rd Query "Death count for the province of each device?"
print("\nQuery 3\n")
reader = snsql.from_df(df, privacy = privacy, metadata=meta_path)

result = reader.execute('\
                        SELECT PREF_DESC_E, COUNT(INCIDENT_ID) AS Death_Count \
                        FROM IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E \
                        == \'DEATH\' GROUP BY PREF_DESC_E ORDER BY Death_Count \
                        DESC LIMIT 5')

print(tabulate(result, headers="firstrow"))

# This is the 1st Query "Non-death incidents for each device"

result2 = reader.execute('SELECT PREF_DESC_E, COUNT(INCIDENT_ID) AS Death_Count FROM IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E != \'DEATH\' GROUP BY PREF_DESC_E ORDER BY Death_Count DESC LIMIT 5')

print("\nQuery 1\n")
print(tabulate(result2, headers="firstrow"))