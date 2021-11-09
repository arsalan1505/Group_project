import pandas as pd
import os
import pymysql
from dotenv import load_dotenv
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="arsalan",
    user="postgres",
    password="password")

curr = conn.cursor()

# curr.execute(open("script.sql", "r").read())

# def inject():


conn.commit()
curr.close()
conn.close()