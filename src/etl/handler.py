import boto3
import csv
import pandas as pd
import numpy as np
import os
import psycopg2
import psycopg2.extras
import sys
from itertools import chain
import uuid

def handle(event, context):
    # List all files in bucket
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket = bucket, Key = key)
    data = s3_object['Body'].read().decode('utf-8')
    csv_data = csv.reader(data.splitlines())

    df = pd.DataFrame (csv_data)
    df.columns =['DateTime', 'Location', 'Name', 'Products', 'Amount', 'PaymentType', 'CardDetails']
    del df['CardDetails'], df['Name']
    transaction, basket = transform(df)
    load(transaction, basket)

def chainer(s):
    return list(chain.from_iterable(s.str.split(',')))

def transform(df):
    # Transaction Table
    transactions_data = df[['DateTime', 'Location', 'PaymentType', 'Amount']]
    uuids = []
    for i in range(len(transactions_data)):
        uuids.append(str(uuid.uuid4()))
    transactions_data['TransactionID'] = uuids

    product_length = df['Products'].str.split(',').map(len)
    #* create new dataframe, repeating or chaining as appropriate
    product_split = pd.DataFrame({'DateTime': np.repeat(df['DateTime'], product_length),
                        'Products': chainer(df['Products'])})
    del product_split['DateTime']
    return transactions_data, product_split

def basket_data(basket):
    basketItems = []
    if type(basket["Products"]) is str:
        basketdict = {}
        if basket["Products"].count("-") == 1:
            splitval = basket["Products"].split("-")
            if "Large" in basket["Products"]:
                basketdict["item_size"] = "Large"
                name = splitval[0].replace("Large", "").strip()
                basketdict["item_name"] = name
            else:
                basketdict["item_size"] = "Regular"
                name = splitval[0].replace("Regular", "").strip()
                basketdict["item_name"] = name
            basketdict["item_price"] = float(splitval[1])
        elif basket["Products"].count("-") == 2:
            splitval = basket["Products"].rsplit("-", 1)
            basketdict["item_price"] = float(splitval[1])
            if "Large" in basket["Products"]:
                basketdict["item_size"] = "Large"
                name = splitval[0].replace("Large", "").strip()
                basketdict["item_name"] = name
            else:
                basketdict["item_size"] = "Regular"
                name = splitval[0].replace("Regular", "").strip()
                basketdict["item_name"] = name
        # basketdict['transaction_index'] = basket[0]
        # return basketdict
        basketItems.append(basketdict)

    else:
        for item in basket.itertuples():
            basketdict = {}
            if item[1].count("-") == 1:
                splitval = item[1].split("-")
                if "Large" in item[1]:
                    basketdict["item_size"] = "Large"
                    name = splitval[0].replace("Large", "").strip()
                    basketdict["item_name"] = name

                else:
                    basketdict["item_size"] = "Regular"
                    name = splitval[0].replace("Regular", "").strip()
                    basketdict["item_name"] = name

                basketdict["item_price"] = float(splitval[1])
            elif item[1].count("-") == 2:
                splitval = item[1].rsplit("-", 1)
                basketdict["item_price"] = float(splitval[1])
                if "Large" in item[1]:
                    basketdict["item_size"] = "Large"
                    name = splitval[0].replace("Large", "").strip()
                    basketdict["item_name"] = name
                else:
                    basketdict["item_size"] = "Regular"
                    name = splitval[0].replace("Regular", "").strip()
                    basketdict["item_name"] = name
            # basketdict['transaction_index'] = item[0]
            basketItems.append(basketdict)
    return basketItems

def load(transactions, whole_basket):
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")
    # get cluster credentials
    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600
        )
    except Exception as e:
        print(e)
        sys.exit(1)
    # create object connection to redshift
    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as e:
        print(e)
        sys.exit(1)

    with conn.cursor() as cursor:

        psycopg2.extras.execute_values(cursor, """INSERT INTO transactions (transaction_id,date_and_time, branch_name, payment_type, total_cost) VALUES %s;""",
        [(
            t[1]["TransactionID"],
            t[1]["DateTime"],
            t[1]["Location"],
            t[1]["PaymentType"],
            t[1]["Amount"]
        )for t in transactions.iterrows()],
        template='(%s,%s,%s,%s,%s)')
        
        for transaction in transactions.iterrows():
            miniBasket = whole_basket.loc[transaction[0]]
            basket_items = basket_data(miniBasket)

            # if type(basket_items) == dict:
            #     psycopg2.extras.execute_values(cursor, """INSERT INTO basket_item (bi_size, bi_name, bi_cost, transaction_id) VALUES %s;""",
            #         [(
            #             basket_items["item_size"],
            #             basket_items["item_name"],
            #             basket_items["item_price"],
            #             returned_id
            #         )]
            #         )
            # else:

            psycopg2.extras.execute_values(cursor, """INSERT INTO basket_item (bi_size, bi_name, bi_cost, transaction_id) VALUES %s;""",
                [(
                    b["item_size"],
                    b["item_name"],
                    b["item_price"],
                    transaction[1]["TransactionID"]
                ) for b in basket_items],
                template='(%s,%s,%s,%s)')

        conn.commit()