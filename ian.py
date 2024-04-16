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
csv_path4 = 'NewCSV/INCIDENT.csv'
csv_path6 = 'NewCSV/PREF_NAME_CODE_TABLE.csv'

mID = 'metaID.yaml'

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
privacy_values = [0.1, 0.5, 1.0]  # Epsilon values to test

#list of triplets with (query, dataframe, metafile)
#add new queries to the list
# QueDfPath = [(DPSD, dfDPSD, mDPSD), (DPC, dfDPC, mDPC)]
QueDfPath = [(NFIPD, dfNFIPD, mNFIPD)]

def run_accuracy(epsilon):
    privacy = Privacy(epsilon=epsilon)
    reader = snsql.from_df(dfNFIPD,privacy=privacy, metadata=mNFIPD)
    query_results = [[] for _ in range(10)]
    for i in range(10):
        results = reader.execute(NFIPD)
        
        results = results[1:len(results)]
        query_results[i].append([inner_list[0] for inner_list in results])

    return query_results


NFIPD = 'SELECT COUNT(INCIDENT_ID) AS \
        NonFatal_Incident_Count FROM IJOINID.PUMS AS I WHERE \
        I.HAZARD_SEVERITY_CODE_E != \'DEATH\' GROUP BY PREF_DESC_E, TRADE_NAME \
        ORDER BY NonFatal_Incident_Count DESC LIMIT 5'

NFIPDa = 'SELECT COUNT(INCIDENT_ID) AS \
        NonFatal_Incident_Count FROM dfNFIPD AS I WHERE \
        I.HAZARD_SEVERITY_CODE_E != \'DEATH\' GROUP BY PREF_DESC_E, TRADE_NAME \
        ORDER BY NonFatal_Incident_Count DESC LIMIT 5'

error = {epsilon: run_accuracy(epsilon) for epsilon in privacy_values}
actual = ps.sqldf(NFIPDa, globals())
actual_list = actual['NonFatal_Incident_Count'].tolist()
print(actual_list)

absolute_errors = [[] for _ in range(3)]
for sublist in absolute_errors:
    sublist.extend([[] for _ in range(10)])

for i in range(len(absolute_errors)):
    for j in range(len(error[privacy_values[i]])):
        for k in range(len(error[privacy_values[i]][j][0])):
                absolute_errors[i][j].append(error[privacy_values[i]][j][0][k] - actual_list[k])

# print(absolute_errors)
def calculate_variance(data):
    mean = sum(data) / len(data)
    squared_diff = [(x - mean) ** 2 for x in data]
    variance = sum(squared_diff) / len(data)
    return variance

variances = [[] for _ in range(3)]
     
for i in range(len(absolute_errors)):
    for j in range(len(absolute_errors[i])):
        variances[i].append(calculate_variance(absolute_errors[i][j]))

# print(variances)

for i, errors in enumerate(variances):
    plt.plot([privacy_values[i]] * 10, errors, label=f'Epsilon = {privacy_values[i]}', marker='o')

plt.xlabel('Epsilon')
plt.ylabel('Variance')
plt.title('Variance vs. Epsilon for Each Device')
plt.legend()
plt.grid(True)
plt.show()