import snsql
from snsql import Privacy
import pandas as pd
from tabulate import tabulate
import pandasql as ps
from pandasql import sqldf
import time
import matplotlib.pyplot as plt


privacy = Privacy (epsilon = .1)

csv_path2 = 'NewCSV/INCIDENT_DEVICE.csv'

meta_path = 'metaID.yaml'

pums = pd.read_csv(csv_path2)

# reader = snsql.from_df(pums, privacy = privacy, metadata=meta_path)

# actual = ps.sqldf('SELECT DEVICE_ID, COUNT(INCIDENT_ID) AS NUM_INCIDENTS FROM pums GROUP BY DEVICE_ID ORDER BY NUM_INCIDENTS DESC LIMIT 5', globals())
# print(actual)

# results = []
# times = []

# for i in range(10):
#     start = time.time()
#     results.append(reader.execute('SELECT DEVICE_ID, COUNT(INCIDENT_ID) AS NUM_INCIDENTS FROM ID.PUMS GROUP BY DEVICE_ID ORDER BY NUM_INCIDENTS DESC LIMIT 5'))
#     end = time.time()

#     times.append(end - start)

# print(times)

privacy_values = [0.1, 0.5, 1.0]  # Epsilon values to test

def run_query(epsilon):
    privacy = Privacy(epsilon=epsilon)
    reader = snsql.from_df(pums, privacy=privacy, metadata=meta_path)
    times = []
    for i in range(10):
        start = time.time()
        reader.execute('SELECT DEVICE_ID, COUNT(INCIDENT_ID) AS NUM_INCIDENTS FROM ID.PUMS GROUP BY DEVICE_ID ORDER BY NUM_INCIDENTS DESC LIMIT 5')
        end = time.time()
        times.append(end - start)
    return times  # Return execution times for 10 runs

# Run the query for each epsilon value and record execution times
execution_times = {epsilon: run_query(epsilon) for epsilon in privacy_values}

# Plotting
for epsilon, times in execution_times.items():
    plt.scatter([epsilon] * 10, times, label=f'Epsilon = {epsilon}', marker='o')

plt.xlabel('Epsilon')
plt.ylabel('Execution Time (s)')
plt.title('Execution Time vs. Epsilon')
plt.legend()
plt.grid(True)
plt.show()
