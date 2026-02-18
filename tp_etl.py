#=============================================================
# TP1  ETL Beginner - Python + pandas
#=============================================================
# Course : Analyse de donnees avec les outils OLAP
# Professor : Amosse EDOUARD
# Students Full Name :
#                           Jean Michel Ralph MANY
#                           Frantzdy DORILUS
#                           Lovenson JEUDINOR
# Port-au-Prince, Haiti
# feb 2026, 16th
#=============================================================

import os
import pandas as pd
import sqlite3

from tp_etl.tp_etl.etl_test import load_csv

#=============================================================
# FUNCTIONS
#=============================================================

#---------------- extract ----------------
def loading_csv_file(file_name, data_dir="sqlite_exports"):
    """
    Read csv with Pandas and print basic info.
    
    :param path: File name
    """
    path = os.path.join(data_dir, f"{file_name}.csv")
    print(f"\nLoading the {file_name}.csv ...")
    df = pd.read_csv(path, low_memory=False, index_col=0)
    print(f"\n================== Statistics on {file_name} ==================\n")
    print(f"Dimension {df.shape}")
    print(f"\n================== Print the first five (5) rows of {file_name} ==================\n")
    print(df.head())
    print("\n================== Data Types ==================\n")
    print(df.dtypes)

    return df

def extract_data(path):
    df = loading_csv_file(path)
    return df

#---------------- transform ----------------

def convert_dates(df):
    """Convert columns containing 'date' or 'timestamp' to datetime."""
    for col in df.columns:
        if "date" in col.lower() or "timestamp" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def drop_duplicates(data, table_name):
    n_dup = data.duplicated().sum()
    if n_dup > 0:
        print(f"Dropping {n_dup} duplicates in {table_name}.")
        data.drop_duplicates(inplace=True)
    return data
    

def handle_missing_values(df, fill_values=None, median_cols=None):
    """
    df : Dataframe to handle missing values
    fill_values : dict {col: value} -> simple imputation
    mdedian_cols : list -> numeric columns to fill with median values
    """
    if fill_values:
        df = df.fillna(fill_values)

    if median_cols:
        df[median_cols] = df[median_cols].fillna(df[median_cols].median())

    return df


def data_transformation(data, table_name, fill_values=None, median_cols=None):
    data = convert_dates(data)
    data = drop_duplicates(data, table_name)
    data = handle_missing_values(data, fill_values=fill_values, median_cols=median_cols)
    return data


# ---------------- load ----------------

def load_to_csv(df, file_name, output_dir="outputs"):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{file_name}.csv")
    df.to_csv(path, index=False)
    print(f"{file_name}.csv saved to {output_dir}/")

def load_to_sqlite(df, table_name, db_path="outputs/etl.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"{table_name} table saved to {db_path}")
    

#=============================================================
# MAIN ETL PROCESS
#=============================================================
if __name__== "__main__":
    
    data_dir = "sqlite_exports"
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    # extract
    
    customers = load_csv("customers", data_dir)
    sellers = load_csv("sellers", data_dir)
    products = load_csv("products", data_dir)
    orders = load_csv("orders", data_dir)
    order_items = load_csv("order_items", data_dir)
    order_pymnts = load_csv("order_pymts", data_dir)
    order_reviews = load_csv("order_reviews", data_dir)
    geoloc = load_csv("geoloc", data_dir)
    translation = load_csv("translation", data_dir)
    order_pymnts = load_csv("order_pymts", data_dir)

    # transform

    na_rules = {
        "order_reviews": {
            "review_comment_title": "No Title",
            "review_comment_message": "No Comment"
        },

        "products": {
            "product_category_name": "Unknown",
            "product_description_lenght": 0,
            "product_name_lenght": 0,
            "product_photos_qty": 0
        }
    }

    dim_cols = [
        "product_description_lenght",
        "product_name_lenght",
        "product_photos_qty"
    ]


    # apply transformations

    customers = data_transformation(customers, "customers")
    sellers = data_transformation(sellers, "sellers")
    products = data_transformation(products, "products", fill_values=na_rules["products"], median_cols=dim_cols)
    orders = data_transformation(orders, "orders")
    order_items = data_transformation(order_items, "order_items")
    order_pymnts = data_transformation(order_pymnts, "order_pymnts")
    order_reviews = data_transformation(order_reviews, "order_reviews", fill_values=na_rules["order_reviews"])
    geoloc = data_transformation(geoloc, "geoloc")
    translation = data_transformation(translation, "translation")

    # merge translation for products categories
    products = products.merge(translation, on ="product_category_name", how="left") 

    # merge geolocation for customers
    customers = customers.merge(geoloc, left_on="customer_zip_code_prefix", how="left")

    # contruct fact table

    fact_orders = (order_items
                .merge(orders, on="order_id", how="inner")
                .merge(customers, on="customer_id", how="left")
                .merge(products, on="product_id", how="left")
                .merge(sellers, on="seller_id", how="left"))


    # revenue 

    fact_orders["freight_value"] = fact_orders["freight_value"].fillna(0)
    fact_orders["revenue"] = fact_orders["price"] + fact_orders["freight_value"]

    # Month for revenue aggregation
    fact_orders["month"] = fact_orders["order_purchase_timestamp"].dt.to_period("M")
    monthly_revenue = (fact_orders.groupby("month")["revenue"].sum()
                    .reset_index()
                    .rename(columns={"month": "year_month"}))

    print("================== Revenue per month ==================\n")
    print(monthly_revenue)

    # Top categories per revenu
    top_categories = (fact_orders.groupby("product_category_name")["revenue"].sum()
                    .reset_index()
                    .sort_values(by="revenue", ascending=False)
                    .head(10))

    # Delivery metrics
    fact_orders["delivery_time_days"] = (fact_orders["order_delivered_customer_date"] - fact_orders["order_purchase_timestamp"]).dt.days
    delivery_metrics = fact_orders.groupby("month")["delivery_time_days"].mean().reset_index()
    delivery_metrics.rename(columns={"month": "year_month", "delivery_time_days": "avg_delivery_days"}, inplace=True)

    print(f"\nAverage delivery time (days) : {delivery_metrics['avg_delivery_days'].mean():.2f}")

    # Monthly reviews score
    order_reviews["month_review"] = order_reviews["review_answer_timestamp"].dt.to_period("M")
    monthly_reviews = (order_reviews.groupby("month_review")["review_score"].mean()
                       .reset_index()
                       .rename(columns={"month_review": "year_month", "review_score": "avg_review_score"}))
    


    # Load - CSV
    load_to_csv(fact_orders, "fact_order_items", output_dir)
    load_to_csv(monthly_revenue, "monthly_revenue", output_dir)
    load_to_csv(top_categories, "top_categories", output_dir)
    load_to_csv(delivery_metrics, "delivery_metrics", output_dir)
    load_to_csv(monthly_reviews, "review_score_monthly", output_dir)
    load_to_csv(customers, "dim_customers", output_dir)
    load_to_csv(sellers, "dim_sellers", output_dir)
    load_to_csv(products, "dim_products", output_dir)

    # Load - SQLite
    load_to_sqlite(fact_orders, "fact_order_items", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(monthly_revenue, "monthly_revenue", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(top_categories, "top_categories", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(delivery_metrics, "delivery_metrics", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(monthly_reviews, "review_score_monthly", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(customers, "dim_customers", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(sellers, "dim_sellers", os.path.join(output_dir, "etl.db"))
    load_to_sqlite(products, "dim_products", os.path.join(output_dir, "etl.db"))

    print("\nETL completed successfully! CSVs and SQLite database created in 'outputs/'")
