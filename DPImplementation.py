import snsql
from snsql import Privacy
import pandas as pd
privacy = Privacy (epsilon = 1.0, delta=0)

csv_path = '/Users/joepuplava/Documents/Spring_2024/Privacy Security Data/CSV_Versions/COPY_INCIDENT_COMPANY.csv'
meta_path = 'data.yaml'

pums = pd.read_csv(csv_path)
# print(pums)
print (pums.info())
reader = snsql.from_df(pums, privacy = privacy, metadata=meta_path)

# result = reader.execute('SELECT * FROM meta_Incident_Company.public')
