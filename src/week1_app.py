import pandas as pd
import numpy as np
from itertools import chain
# import connections


# #* Reading data
df = pd.read_csv (r'C:\Users\zakda\group_project\team-1-project\src\data\2021-02-23-isle-of-wight.csv')
del df['Card-details'], df['Name']

# #* Transaction Table
Table_1 = df[['Date-Time', 'Location', 'Payment_type', 'Amount']]

# #* Inserting data into "transactions"
i = len(Table_1)
x = 0
while x < i:
    date_and_time = f"'{Table_1['Date-Time'].iloc[x]}'"
    branch_name = f"'{Table_1['Location'].iloc[x]}'"
    payment_type = f"'{Table_1['Payment_type'].iloc[x]}'"
    total_cost = Table_1['Amount'].iloc[x]

    sql = f"INSERT INTO transactions (date_and_time, branch_name, payment_type, total_cost) VALUES ({date_and_time}, {branch_name}, {payment_type}, {total_cost});" # TODO
    connections.cursor.execute(sql)

    x += 1
    # if x == 2:
    #     break

# #* Counting payment types
table_3 = df.groupby(["Payment_type"]).agg({"Payment_type": "count"})


#*Stacks and keeps orders together but on each row
def chainer(s):
    return list(chain.from_iterable(s.str.split(',')))

lens = df['Products'].str.split(',').map(len)
#* create new dataframe, repeating or chaining as appropriate
res = pd.DataFrame({'Date-Time': np.repeat(df['Date-Time'], lens),
                    'Products': chainer(df['Products'])})
temp = res.reset_index()
print(temp)

i = len(temp)

x = 0
tempString = ''
while x < i:
    if temp['Products'].iloc[x] == '':
        tempString += "'Small', "
    else:
        tempString += f"'{temp['Products'].iloc[x]}', "
    if x%3 == 2:
        tempString += f"{temp['index'].iloc[x] + 1}"
        print(tempString)
    #     sql = f"INSERT INTO basket_item (bi_size, bi_name, bi_cost, transaction_id) VALUES ({tempString});" # TODO
    #     # print(sql)
    #     tempString = ''
    #     # print('\n')
    #     connections.cursor.execute(sql)
    # x += 1
    # # if x == 13:
    # #     break

