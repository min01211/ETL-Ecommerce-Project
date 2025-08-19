import os
import pandas as pd
import matplotlib as plt
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#Setting the diresction 
BASE = os.path.dirname(os.path.dirname("C:/Users/dlwhd/OneDrive/바탕 화면/Projects/ETL Ecommerce Project"))
OUT = os.path.join(BASE, "datesets", "processed")
os.makedirs(OUT, exist_ok = True)

#Connect mySQL
USER = "root"
PASSWORD = quote_plus("Happydk@1")
HOST = "localhost"
DB = "ecommerce_db"
engine = create_engine("mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}")




