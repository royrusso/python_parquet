import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CSV_FILE = 'people-1000.csv'
PARQUET_FILE = 'people-1000.parquet'

# read CSV into pandas
df = pd.read_csv(CSV_FILE)

# convert to pyarrow table
table = pa.Table.from_pandas(df)

# write to parquet
pq.write_table(table, PARQUET_FILE)

# read parquet file
table2 = pq.read_table(PARQUET_FILE)

# convert to pandas
df2 = table2.to_pandas()

print("\n\nDataframe:\n", df2.head())

print("\n\nSchema:\n", table2.schema)
print("\n\nMetadata:\n", table2.schema.metadata)

pfile = pq.ParquetFile(PARQUET_FILE)
print("\n\nParquet Metadata:\n",pfile.metadata)
print("\n\nParquet Schema:\n",pfile.schema)
print("\n\nRow Group Metadata:\n",pfile.metadata.row_group(0))
print("\n\nColumn Metadata:\n",pfile.metadata.row_group(0).column(0))
