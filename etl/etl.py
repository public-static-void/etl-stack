#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Summary       : Extract data from source and load it into destination DB.

Author        : Vadim Titov
Created       : Wed Jun 26 19:57:49 2024 +0200
Last modified : Tue Jul 02 14:22:37 2024 +0200
"""

import os
import sys
import time

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()

ETL_USER = os.environ["ETL_USER"]
ETL_PASS = os.environ["ETL_PASS"]
PG_PORT = os.environ["PG_PORT"]
SA_PORT = os.environ["SA_PORT"]
DRIVER = "{ODBC Driver 18 for SQL Server}"
SA_DB = "AdventureWorksDW2022"
PG_DB = "AdventureWorksDW2022PY"
SA_SERVER = "sqlserver"
PG_SERVER = "postgres"


def init_src_conn() -> sqlalchemy.engine.base.Engine:
    """
    Create connection to the source DB.

    Returns
    -------
    sqlalchemy.engine.base.Engine
        Connection to the source DB.
    """
    conn_str = f"DRIVER={DRIVER};SERVER={SA_SERVER};PORT={SA_PORT};DATABASE={SA_DB};UID={ETL_USER};PWD={ETL_PASS};TrustServerCertificate=yes"
    conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
    src_engine = create_engine(conn_url)
    return src_engine


def init_dst_conn() -> sqlalchemy.engine.base.Engine:
    """
    Create connection to the destination DB.

    Returns
    -------
    sqlalchemy.engine.base.Engine
        Connection to the destination DB.
    """
    conn_url = (
        f"postgresql://{ETL_USER}:{ETL_PASS}@{PG_SERVER}:{PG_PORT}/{PG_DB}"
    )
    dst_engine = create_engine(conn_url)
    return dst_engine


def extract(
    src_conn: sqlalchemy.engine.base.Engine,
) -> list[tuple[pd.DataFrame, str]]:
    """
    Extract data from source DB.

    Parameters
    ----------
    src_conn : sqlalchemy.engine.base.Engine
        Connection to the source DB.

    Returns
    -------
    list[tuple[pd.DataFrame, str]]
        Dataframes and table names from the source DB.
    """
    raw_data: list[tuple[pd.DataFrame, str]] = []
    query = """SELECT t.name
                AS table_name
                FROM sys.tables t
                WHERE t.name
                IN ('DimProduct',
                    'DimProductCategory',
                    'DimProductSubcategory'
                    )"""
    src_tables = pd.read_sql_query(query, src_conn).to_dict()["table_name"]
    for table_id in src_tables:
        table_name = src_tables[table_id]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", src_conn)
        print(f"processing {table_name}")
        raw_data.append((df, table_name))
    return raw_data


def transform(
    raw_data: list[tuple[pd.DataFrame, str]],
) -> list[tuple[pd.DataFrame, str]]:
    """
    Transform data.

    Parameters
    ----------
    raw_data : list[tuple[pd.DataFrame, str]]
        Dataframes and table names from the source DB.

    Returns
    -------
    list[tuple[pd.DataFrame, str]]
        Transformed dataframes and table names.
    """
    tf_data: list[tuple[pd.DataFrame, str]] = []
    for df, table_name in raw_data:
        if table_name == "DimProduct":
            df = tf_product(df)
        if table_name == "DimProductCategory":
            df = tf_product_category(df)
        if table_name == "DimProductSubcategory":
            df = tf_product_subcategory(df)
        tf_data.append((df, table_name))
    return tf_data


def tf_product(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "ProductKey",
            "ProductAlternateKey",
            "ProductSubcategoryKey",
            "WeightUnitMeasureCode",
            "SizeUnitMeasureCode",
            "EnglishProductName",
            "StandardCost",
            "FinishedGoodsFlag",
            "Color",
            "SafetyStockLevel",
            "ReorderPoint",
            "ListPrice",
            "Size",
            "SizeRange",
            "Weight",
            "DaysToManufacture",
            "ProductLine",
            "DealerPrice",
            "Class",
            "Style",
            "ModelName",
            "EnglishDescription",
            "StartDate",
            "EndDate",
            "Status",
        ]
    ]
    df["WeightUnitMeasureCode"].fillna("NA", inplace=True)
    df["ProductSubcategoryKey"].fillna("0", inplace=True)
    df["SizeUnitMeasureCode"].fillna("NA", inplace=True)
    df["StandardCost"].fillna("0", inplace=True)
    df["ListPrice"].fillna("0", inplace=True)
    df["ProductLine"].fillna("NA", inplace=True)
    df["Class"].fillna("NA", inplace=True)
    df["Style"].fillna("NA", inplace=True)
    df["Size"].fillna("NA", inplace=True)
    df["ModelName"].fillna("NA", inplace=True)
    df["EnglishDescription"].fillna("NA", inplace=True)
    df["DealerPrice"].fillna("0", inplace=True)
    df["Weight"].fillna("0", inplace=True)
    df["EndDate"].fillna("NA", inplace=True)
    df["Status"].fillna("NA", inplace=True)
    df = df.rename(
        columns={
            "EnglishDescription": "Description",
            "EnglishProductName": "ProductName",
        }
    )
    df["ProductSubcategoryKey"] = df["ProductSubcategoryKey"].astype(int)
    df["StandardCost"] = (
        df["StandardCost"].astype(float).apply("{:.02f}".format)
    )
    df["ListPrice"] = df["ListPrice"].astype(float).apply("{:.02f}".format)
    return df


def tf_product_category(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "ProductCategoryKey",
            "ProductCategoryAlternateKey",
            "EnglishProductCategoryName",
        ]
    ]
    df = df.rename(
        columns={"EnglishProductCategoryName": "ProductCategoryName"}
    )
    return df


def tf_product_subcategory(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "ProductSubcategoryKey",
            "EnglishProductSubcategoryName",
            "ProductSubcategoryAlternateKey",
            "ProductCategoryKey",
        ]
    ]
    # Rename columns with rename function
    df = df.rename(
        columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"}
    )
    return df


def load(
    engine: sqlalchemy.engine.base.Engine, df: pd.DataFrame, table_name: str
) -> None:
    """
    Load data into destination DB.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        Connection to the destination DB.
    df : pd.DataFrame
        Data to be loaded.
    table_name : str
        Name of the destination table.
    """
    rows_imported = 0
    print(
        f"importing rows {rows_imported} to {rows_imported + len(df)}..."
        f" for table {table_name}"
    )
    df.to_sql(f"stg_{table_name}", engine, if_exists="replace", index=False)
    rows_imported += len(df)
    print(f"Done. Imported {rows_imported} rows.")


def main():
    """Extract data from source and load it into destination DB."""
    success = False
    max_attempts = 100
    print(25 * "=")
    print("Initiating stage 1/5 (Connecting to source DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to connect to source DB...")
        print(f"Attempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            src_conn = init_src_conn()
            print(25 * "=")
            print("Stage 1/5 successful")
            break
        except Exception as e:
            print("Error connecting to source DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 2/5 (Extracting data from source DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to extract data from source DB...")
        print(f"Attempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            extracted_data = extract(src_conn)
            print(25 * "=")
            print("Stage 2/5 successful")
            break
        except Exception as e:
            print("Error extracting data from source DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 3/5 (Transforming data)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to transform data...")
        print(f"Attempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            transformed_data = transform(extracted_data)
            print(25 * "=")
            print("Stage 3/5 successful")
            break
        except Exception as e:
            print("Error transforming data:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 4/5 (Connecting to destination DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to connect to destination DB...")
        print(f"Attempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            dst_conn = init_dst_conn()
            print(25 * "=")
            print("Stage 4/5 successful")
            break
        except Exception as e:
            print("Error connecting to destination DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 5/5 (Loading data into destination DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to write data into destination DB...")
        print(f"Attempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            for df, table_name in transformed_data:
                load(dst_conn, df, table_name)
            print(25 * "=")
            print("Stage 5/5 successful")
            success = True
            break
        except Exception as e:
            print("Error loading data into destination DB:", repr(e))
            time.sleep(3)

    exit_code = 0 if success else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
