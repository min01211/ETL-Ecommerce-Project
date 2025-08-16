import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import quote_plus

####Extract#####
#Setting Datasets Path / DB Setting
Base = "C:/Users/dlwhd/OneDrive/바탕 화면/Projects/ETL Ecommerce Project"
Raw = os.path.join(Base, "datasets", "raw")
Processed = os.path.join(Base, "datasets", "processed")

#Setting MySQL Path
Password = quote_plus("Happydk@1")
engine = create_engine("mysql+mysqlconnector://root:Password@localhost/ecommerce_db")

#Read CSV Files
orders = pd.read_csv(os.path.join(Raw, "olist_orders_dataset.csv"),
parse_dates = ["order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",],
dtype = {"order_id":"string", "customer_id": "string", "order_status": "category"})

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

sellers = pd.read_csv(os.path.join(Raw, "olist_sellers_dataset.csv"),
dtype = {"seller_id":"string",
         "seller_city":"category",
         "seller_state":"category"})

geolocations = pd.read_csv(os.path.join(Raw, "olist_geolocation_dataset.csv"),
dtype = {"geolocation_city":"category",
         "geolocation_state":"category"})

#drop duplicates, drop n/a
orders = orders.drop_duplicates().dropna()
items = items.drop_duplicates().dropna()
payments = payments.drop_duplicates().dropna()
customers = customers.drop_duplicates().dropna()
products = products.drop_duplicates().dropna()
translations = translations.drop_duplicates().dropna()
sellers = sellers.drop_duplicates().dropna()
geolocations = geolocations.drop_duplicates().dropna()

####Transfer#####
#1. Filtering the order data that is completed
orders_delivered = orders[orders["order_status"] == "delivered"].copy()

#2. Adding Enligsh translation to each products / #.merge(#1, on = "column that has translated data from #1", how = left or right)
products = products.merge(translations, on = "product_category_name", how = "left")

#3. Calculating each item's price and freight value (converting string to float)
items["line_revenue"] = items["price"].astype(float)
items["line_freight"] = items["freight_value"].astype(float)

#4. Merging customer info into deliver_completed data
orders_cust = orders_delivered.merge(customers, on = "customer_id", how = "left")

#5. Filtering sum of prices and freight valeus by order id
order_items_agg = (items.groupby("order_id", as_index = False)).agg(
        total_sales = ("line_revenue", "sum"),
        total_freight_values = ("line_freight", "sum"),
        total_count = ("order_id", "count")
)

#6. Combining orders and payments what customers paid
pay_agg = (payments.groupby("order_id", as_index = False)).agg(
        total_paid = ("payment_value", "sum")
)

#7. Combining order_delivered with customers info, items price info, and payments info
fact_orders = (
        orders_cust.merge(order_items_agg, on = "order_id", how = "left")
                   .merge(pay_agg, on = "order_id", how = "left"                        
                   ))
        
#8. Creating new columns including each order month and shpping days
#dt.to_period('M') <= shows only YYYY/MM
#Shipping days = order delivered date - order purchase date
fact_orders["order_month"] = fact_orders["order_purchase_timestamp"].dt.to_period('M').astype(str)
fact_orders["shipping_days"] = (fact_orders["order_delivered_customer_date"] - fact_orders["order_purchase_timestamp"]).dt.days

#9 drop n/a
fact_orders = fact_orders.dropna(subset = ["total_sales", "total_paid", "order_purchase_timestamp"])
fact_orders = fact_orders[fact_orders["total_sales"] >= 0]

#10 Filtering most sold items
#merge function. #[[colum1, colum2]] <= if I want to use only 2 columns from the dataset
items_prod = items.merge(products[["product_id", "product_category_name_english"]], on = "product_id", how = "left")
cat_sales = (items_prod.groupby("product_category_name_english", as_index = False, observed = False)
        .agg(revenue = ("line_revenue", "sum"), items = ("order_item_id", "count"))
        .sort_values("revenue", ascending = False))
          
#11





