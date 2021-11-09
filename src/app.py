import pandas as pd
import numpy as np
from itertools import chain

# #* Reading data (You will have to alter the path to your own Csv File location.. right click on your csv file and copy path)
df = pd.read_csv (r'C:\Users\zakda\group_project\team-1-project\src\data\2021-02-23-isle-of-wight.csv')
del df['Card-details'], df['Name']

# #* Transaction Table
Table_1 = df[['Date-Time', 'Location', 'Payment_type', 'Amount']]
print (Table_1)

# #* Counting payment types
table_3 = df.groupby(["Payment_type"]).agg({"Payment_type": "count"})
print (table_3)

#*Stacks and keeps orders together but on each row
def chainer(s):
    return list(chain.from_iterable(s.str.split(',')))
lens = df['Products'].str.split(',').map(len)
# create new dataframe, repeating or chaining as appropriate
res = pd.DataFrame({'Date-Time': np.repeat(df['Date-Time'], lens),
                    'Products': chainer(df['Products'])})
temp = res.reset_index()

i = len(temp)
print(i)
x = 0
while x < i:
    # if x%3 == 0:
    #     print('Size:')
    # elif x%3 == 1:
    #     print('Name:')
    # elif x%3 == 2:
    #     print('Price:')
    
    if temp['Products'].iloc[x] == '':
        print('Small')
    else:
        print(temp['Products'].iloc[x])
    x += 1
    if x%3 == 0:
        print('\n')

print (temp)
#transaction - basket data(relationship to transaction)
