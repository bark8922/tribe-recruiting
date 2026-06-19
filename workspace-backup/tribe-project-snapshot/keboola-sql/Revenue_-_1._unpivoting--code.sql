-- Transformation: Revenue - 1. unpivoting
-- Block: Block 1
-- Code: code
-- Extracted from Keboola on 2026-03-30

import pandas as pd

df = pd.read_csv('in/tables/client-invoiced.csv')
df_unpivot = pd.melt(df, id_vars=['Bubble_ID', 'Company'], value_vars=df.columns[2:], var_name='month', value_name='eur_amount')
df_unpivot.to_csv('out/tables/client-invoiced-unpivoted.csv', index=False) 