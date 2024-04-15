import snsql
from snsql import Privacy
import pandas as pd
from tabulate import tabulate
import pandasql as ps
from pandasql import sqldf
import time
import matplotlib.pyplot as plt
privacy = Privacy (epsilon = 1.0)

csv_path1 = 'NewCSV/INCIDENT_COMPANY.csv'
csv_path2 = 'NewCSV/INCIDENT_DEVICE.csv'
# csv_path3 = 'NewCSV/INCIDENT_PATIENT_DEVICE_CODE.csv'
csv_path4 = 'NewCSV/INCIDENT.csv'
# csv_path5 = 'NewCSV/PATIENT_DEVICE_CODE_TABLE.csv'
csv_path6 = 'NewCSV/PREF_NAME_CODE_TABLE.csv'

# meta_path1 = 'metaIC.yaml'
mID = 'metaID.yaml'
# meta_path3 = 'metaIPDC.yaml'
# meta_path4 = 'metaI.yaml'
# meta_path5 = 'metaPDCT.yaml'
# meta_path6 = 'metaPNCT.yaml'
# metalist   = [meta_path1, meta_path2, meta_path3, meta_path4, meta_path5, meta_path6]

# meta_path = 'IJOINID.yaml'

############################################################################
#Query for finding the non-fatal incidents by device
ID   = pd.read_csv(csv_path2)
I    = pd.read_csv(csv_path4)
PNCT = pd.read_csv(csv_path6)

NFIPD = 'SELECT PREF_DESC_E, TRADE_NAME, COUNT(INCIDENT_ID) AS \
        NonFatal_Incident_Count FROM IJOINID.PUMS AS I WHERE \
        I.HAZARD_SEVERITY_CODE_E != \'DEATH\' GROUP BY PREF_DESC_E, TRADE_NAME \
        ORDER BY NonFatal_Incident_Count DESC LIMIT 5'

mNFIPD = 'IJOINID.yaml'

Ie    = I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E']]
IDe   = ID[['INCIDENT_ID', 'TRADE_NAME', 'PREF_NAME_CODE']]
PNCTe = PNCT[['PREF_NAME_CODE', 'PREF_DESC_E']]

df = pd.merge(Ie, IDe, on='INCIDENT_ID', how='left')
dfNFIPD = pd.merge(df, PNCTe, on='PREF_NAME_CODE', how='left')
print("NFIPD made")

############################################################################
#Query for finding the death count per province by device
ID   = pd.read_csv(csv_path2)
I    = pd.read_csv(csv_path4)
PNCT = pd.read_csv(csv_path6)

DPDD = 'SELECT PREF_DESC_E, TRADE_NAME, Province, COUNT(INCIDENT_ID) AS Death_Count FROM \
        IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' GROUP BY \
        PREF_DESC_E, TRADE_NAME, Province ORDER BY Death_Count DESC LIMIT 5'

mDPDD = 'IJOINID.yaml'

Ie    = I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E', 'Province']]
IDe   = ID[['INCIDENT_ID', 'TRADE_NAME', 'PREF_NAME_CODE']]
PNCTe = PNCT[['PREF_NAME_CODE', 'PREF_DESC_E']]

df = pd.merge(Ie, IDe, on='INCIDENT_ID', how='inner')
dfDPPD = pd.merge(df, PNCTe, on='PREF_NAME_CODE', how='inner')
print("DPPD made")

############################################################################
#Query for finding the death count per sex and per device
ID   = pd.read_csv(csv_path2)
I    = pd.read_csv(csv_path4)
PNCT = pd.read_csv(csv_path6)

DPSD = 'SELECT PREF_DESC_E, TRADE_NAME, Sex, COUNT(INCIDENT_ID) AS Death_Count \
        FROM IJOINID.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
            I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
        GROUP BY PREF_DESC_E, TRADE_NAME, Sex ORDER BY Death_Count DESC LIMIT 8'

mDPSD = 'IJOINID.yaml'

Ie    = I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E', 'Sex']]
IDe   = ID[['INCIDENT_ID', 'TRADE_NAME', 'PREF_NAME_CODE']]
PNCTe = PNCT[['PREF_NAME_CODE', 'PREF_DESC_E']]

df = pd.merge(Ie, IDe, on='INCIDENT_ID', how='inner')
dfDPSD = pd.merge(df, PNCTe, on='PREF_NAME_CODE', how='inner')
print("DPSD made")

############################################################################
#Query for finding the death count per company
DPC = '\
        SELECT COMPANY_NAME, COUNT(INCIDENT_ID) AS Death_Count \
        FROM IJOINIC.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
            I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
        GROUP BY COMPANY_NAME ORDER BY Death_Count DESC LIMIT 5'

mDPC = 'IJOINIC.yaml'

IC = pd.read_csv(csv_path1)

Ie  = I[['INCIDENT_ID', 'HAZARD_SEVERITY_CODE_E']]
ICe = IC[['INCIDENT_ID', 'COMPANY_NAME']]

dfDPC = pd.merge(Ie, ICe, on='INCIDENT_ID', how='inner')
print("DPC made")

############################################################################
#Query for the incident count per device
ICD = 'SELECT DEVICE_ID, COUNT(INCIDENT_ID) AS NUM_INCIDENTS FROM ID.PUMS \
       GROUP BY DEVICE_ID ORDER BY NUM_INCIDENTS DESC LIMIT 5'
mICD = 'ICD.yaml'

dfICD = ID[['INCIDENT_ID', 'DEVICE_ID']]
print("ICD made")
############################################################################
privacy_values = [0.1, 0.5, 1.0]  # Epsilon values to test

#list of triplets with (query, dataframe, metafile)
#add new queries to the list
QueDfPath = [(NIPD, dfNFIPD, mNFIPD), (ICD, dfICD, mICD), (DPDD, dfDPDD, mDPDD), (DPSD, dfDPSD, mDPSD), (DPC, dfDPC, mDPC)]

def run_query(epsilon):
    privacy = Privacy(epsilon=epsilon)  # Assuming Privacy is defined elsewhere
    times = []
    for (query, df, meta) in QueDfPath:  # Assuming QueDfPath is defined elsewhere
        reader = snsql.from_df(df, privacy=privacy, metadata=meta)
        for i in range(10):
            start = time.time()
            reader.execute(query)
            end = time.time()
            times.append(end - start)
        print(f"Execution times for query '{query}' with epsilon={epsilon}: {times}\n")
    return times
# Run the query for each epsilon value and record execution times
execution_times = []
for epsilon in privacy_values:
    execution_times.append((epsilon, run_query(epsilon)))

# Plotting
print(execution_times)
for epsilon, times in execution_times:
    plt.scatter([epsilon] * 50, times, label=f'Epsilon = {epsilon}', marker='o')

plt.xlabel('Epsilon')
plt.ylabel('Execution Time (s)')
plt.title('Execution Time vs. Epsilon')
plt.legend()
plt.grid(True)
plt.show()
