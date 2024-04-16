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
privacy_values = [1.0, 0.5, 0.1]  # Epsilon values to test

#list of triplets with (query, dataframe, metafile)
#add new queries to the list
# QueDfPath = [(DPSD, dfDPSD, mDPSD), (DPC, dfDPC, mDPC)]
QueDfPath = [(DPC, dfDPC, mDPC)]

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
    plt.scatter([epsilon] * 10, times, label=f'Epsilon = {epsilon}', marker='o')

plt.xlabel('Epsilon')
plt.ylabel('Execution Time (s)')
plt.title('Execution Time vs. Epsilon')
plt.legend()
plt.grid(True)
plt.show()



# # accuracy graph
# def run_accuracyDPC(epsilon):
#     privacy = Privacy(epsilon=epsilon)
#     reader = snsql.from_df(dfDPSD, privacy=privacy, metadata=mDPSD)
#     query_results = [[] for _ in range(10)]
#     for i in range(10):
#         results = reader.execute(DPSD)
#         results = results[1:len(results)]
#         query_results[i].append([inner_list[0] for inner_list in results])

#     return query_results

# error = {epsilon: run_accuracyDPC(epsilon) for epsilon in privacy_values}
# actual = ps.sqldf(DPSD, globals())
# actual_list = actual['Death Count Per Sex and Device'].tolist()
# print(actual_list)

# absolute_errors = [[] for _ in range(10)]
# for epsilon, results in error.items():
#     for result in results:
#         for i in range(len(result[0])):
#             absolute_errors[i].append(result[0][i] - actual_list[i])

# print(absolute_errors)


# def run_accuracy(epsilon):
#     privacy = Privacy(epsilon=epsilon)
#     reader = snsql.from_df(dfDPC,privacy=privacy, metadata=mDPC)
#     query_results = [[] for _ in range(10)]
#     for i in range(10):
#         results = reader.execute(DPC)
        
#         results = results[1:len(results)]
#         query_results[i].append([inner_list[0] for inner_list in results])

#     return query_results

# DPC = '\
#         SELECT COUNT(INCIDENT_ID) AS Death_Count \
#         FROM IJOINIC.PUMS AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
#             I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
#         GROUP BY COMPANY_NAME ORDER BY Death_Count DESC LIMIT 5'

# DPCa = 'SELECT COUNT(INCIDENT_ID) AS Death_Count \
#         FROM dfDPC AS I WHERE I.HAZARD_SEVERITY_CODE_E = \'DEATH\' OR \
#             I.HAZARD_SEVERITY_CODE_E = \'POTENTIAL FOR DEATH/INJURY \' \
#         GROUP BY COMPANY_NAME ORDER BY Death_Count DESC LIMIT 5'
# error = {epsilon: run_accuracy(epsilon) for epsilon in privacy_values}
# actual = ps.sqldf(DPCa, globals())
# actual_list = actual['Death_Count'].tolist()
# print(actual_list)

# absolute_errors = [[] for _ in range(3)]
# for sublist in absolute_errors:
#     sublist.extend([[] for _ in range(10)])

# for i in range(len(absolute_errors)):
#     for j in range(len(error[privacy_values[i]])):
#         for k in range(len(error[privacy_values[i]][j][0])):
#                 absolute_errors[i][j].append(error[privacy_values[i]][j][0][k] - actual_list[k])

# # print(absolute_errors)
# def calculate_variance(data):
#     mean = sum(data) / len(data)
#     squared_diff = [(x - mean) ** 2 for x in data]
#     variance = sum(squared_diff) / len(data)
#     return variance

# variances = [[] for _ in range(3)]
     
# for i in range(len(absolute_errors)):
#     for j in range(len(absolute_errors[i])):
#         variances[i].append(calculate_variance(absolute_errors[i][j]))

# # print(variances)

# for i, errors in enumerate(variances):
#     plt.plot([privacy_values[i]] * 10, errors, label=f'Epsilon = {privacy_values[i]}', marker='o')

# plt.xlabel('Epsilon')
# plt.ylabel('Variance')
# plt.title('Variance vs. Epsilon for Each Device')
# plt.legend()
# plt.grid(True)
# plt.show()
