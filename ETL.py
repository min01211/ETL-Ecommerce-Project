import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import quote_plus

####Extract#####
#Setting Datasets Path / DB Setting
Base = "C:/Users/dlwhd/OneDrive/바탕 화면/Projects/ETL Project"
Raw = os.path.join(Base, "datasets", "raw")
Processed = os.path.join(Base, "datasets", "processed")

#Setting MySQL Path
Password = quote_plus("Happydk@1")
engine = create_engine("mysql+mysqlconnector://root:Password@localhost/ecommerce_db")

#Read CSV Files
order = pd.read_csv(os.path.join(Raw, "olist_orders_dataset.csv"), 
parse_dates = ["order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",],
dtype = {"order_id":"string", "order_id": "string", "customer_id": "string", "order_status": "category"})

items = pd.read_csv(os.path.join(Raw, "olist_order_items_dataset.csv"),
parse_dates = ["shipping_limit_date"],
dtype = {"order_id":"string","order_item_id":"string","product_id":"string","seller_id":"string"})

payments = pd.read_csv(os.path.join(Raw, "olist_order_payments_dataset.csv"),
dtype = {"order_id":"string",
          "payment_type":"category",
          "payment_installments":"int16"})

customers = pd.read_csv(os.path.join(Raw, "olist_customers_dataset.csv"),
dtype = {"customer_id":"string",
         "customer_unique_id":"string",
         "customer_city":"category",
         "customer_state":"category"})

products = pd.read_csv(os.path.join(Raw, "olist_products_dataset.csv"),
dtype = {"product_id":"string",
         "product_category_name":"category"})

translations = pd.read_csv(os.path.join(Raw, "product_category_name_translation.csv"),
dtype = {"product_category_name":"string",
         "product_category_name_english":"category"})   

selllers = pd.read_csv(os.path.join(Raw, "olist_sellers_dataset.csv"),
dtype = {"seller_id":"string",
         "seller_city":"category",
         "seller_state":"category"})

geolocations = pd.read_csv(os.path.join(Raw, "olist_geolocation_dataset.csv"),
dtype = {"geolocation_city":"category",
         "geolocation_state":"category"})

####Transform#####




