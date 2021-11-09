# import boto3
import csv
import pandas as pd
# import numpy as np
import os
# import psycopg2
# import psycopg2.extras
import sys
# from itertools import chain
import handler as hnd

df = pd.read_csv(r'chesterfield_10-03-2021_22-37-00.csv')
df.columns =['DateTime', 'Location', 'Name', 'Products', 'Amount', 'PaymentType', 'CardDetails']
del df['CardDetails'], df['Name']

testing,temp_a = hnd.transform(df)
# print (temp_a)

basket_test = hnd.basket_data(temp_a)
# print (basket_test)

basketOfTransaction = hnd.basket_data(temp_a.loc[0])
print (basketOfTransaction)



