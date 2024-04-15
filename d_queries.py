import snsql
from snsql import Privacy
import pandas as pd
from tabulate import tabulate
privacy = Privacy (epsilon = .1)

csv_path2 = 'NewCSV/INCIDENT_DEVICE.csv'

meta_path = 'metaID.yaml'

pums = pd.read_csv(csv_path2)

reader = snsql.from_df(pums, privacy = privacy, metadata=meta_path)

result = reader.execute('SELECT DEVICE_ID, COUNT(INCIDENT_ID) AS NUM_INCIDENTS FROM ID.PUMS GROUP BY DEVICE_ID ORDER BY NUM_INCIDENTS DESC LIMIT 5')    

print(tabulate(result, headers="firstrow"))