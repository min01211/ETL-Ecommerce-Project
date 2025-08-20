import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#Setting the diresction 
BASE = "C:/Users/dlwhd/OneDrive/바탕 화면/Projects/ETL Ecommerce Project"
OUT = os.path.join(BASE, "datasets", "processed")
os.makedirs(OUT, exist_ok = True)

#Connect mySQL
USER = "root"
PASSWORD = quote_plus("Happydk@1")
HOST = "localhost"
DB = "ecommerce_db"
engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}")

#Loading sql datasets
monthly = pd.read_sql("SELECT * FROM agg_monthly_sales ORDER BY order_month", engine)
cat = pd.read_sql("SELECT * FROM agg_category_sales ORDER BY revenue DESC", engine)
state = pd.read_sql('SELECT * FROM agg_state_sales ORDER BY revenue DESC', engine)

#Getting KPI values
total_revenue = monthly["revenue"].sum()
total_orders = monthly["orders"].sum()
aov = total_revenue / total_orders if total_orders else 0

#Prepare figure=> Overall figsize and subtitle
plt.figure(figsize=(14,9))
plt.suptitle("E-Commerce Dashboard (OList)", fontsize = 16, y = 0.98)

#Create line chart in ax1
ax1 = plt.subplot(2, 2, 1)
ax1.plot(monthly["order_month"], monthly["revenue"])
ax1.set_title("Monthly Revenue")
ax1.set_xlabel("Month")
ax1.set_ylabel("Revenue")
ax1.tick_params (axis = 'x', rotation = 45)

#Create horizon bar chart in ax2
topN = 10
#Shows top 10 and reverse the order
cat_top = cat.head(topN).iloc[::-1]
ax2 = plt.subplot(2, 2, 2)
ax2.barh(cat_top["product_category_name_english"].fillna("Unknown"), cat_top["revenue"])
ax2.set_title(f"Top {topN} Categories by Revenue")
ax2.set_xlabel("Revnue")
ax2.set_ylabel("Category")

#Create horizon bar chart(Revenue by State) in ax3
state_top = state.head(topN).iloc[::-1]
ax3 = plt.subplot(2, 2, 3)
ax3.barh(state_top["customer_state"].fillna("N/A"), state_top["revenue"])
ax3.set_title("Top {topN} States by Revenue")
ax3.set_xlabel("Revenue")
ax3.set_ylabel("State")

#Create KPI box instead of graph
ax4 = plt.subplot(2, 2, 4)
ax4.axis("off")
kpi_text = (f"Total Revenue: {total_revenue:,.0f}\n"
            f"Total Orders : {int(total_orders):,}\n"
            f"Avg Order Value (AOV) : {aov:,.2f}")
ax4.text(0.0, 0.8, "Key Metrics", fontsize = 14, weight = "bold")
ax4.text(0.0, 0.6, kpi_text, fontsize = 12, va = "top")

#Storing
plt.tight_layout(rect=[0, 0, 1, 0.96])
out_path = os.path.join(OUT, "dashboard.png")
plt.savefig(out_path, dpi=150)