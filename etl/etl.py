#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Summary       : Extract data from source and load it into destination DB.

Author        : Vadim Titov
Created       : Wed Jun 26 19:57:49 2024 +0200
Last modified : Sat Jun 29 20:42:06 2024 +0200
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
DATABASE = "AdventureWorksDW2022"
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
    conn_str = f"DRIVER={DRIVER};SERVER={SA_SERVER};PORT={SA_PORT};DATABASE={DATABASE};UID={ETL_USER};PWD={ETL_PASS};TrustServerCertificate=yes"
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
        f"postgresql://{ETL_USER}:{ETL_PASS}@{PG_SERVER}:{PG_PORT}/{DATABASE}"
    )
    src_engine = create_engine(conn_url)
    return src_engine


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
    res: list[tuple[pd.DataFrame, str]] = []
    query = """SELECT t.name
                AS table_name
                FROM sys.tables t
                WHERE t.name
                IN ('DimProduct',
                    'DimProductSubcategory',
                    'DimProductSubcategory',
                    'DimProductCategory',
                    'DimSalesTerritory',
                    'FactInternetSales')"""
    src_tables = pd.read_sql_query(query, src_conn).to_dict()["table_name"]
    for table_id in src_tables:
        table_name = src_tables[table_id]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", src_conn)
        print(f"processing {table_name}")
        res.append((df, table_name))
    return res


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
    print("Initiating stage 1/4 (Connecting to source DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to connect to source DB...")
        print(f"Attmempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            src_conn = init_src_conn()
            print(25 * "=")
            print("Stage 1/4 successful")
            break
        except Exception as e:
            print("Error connecting to source DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 2/4 (Extracting data from source DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to extract data from source DB...")
        print(f"Attmempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            extracted_data = extract(src_conn)
            print(25 * "=")
            print("Stage 2/4 successful")
            break
        except Exception as e:
            print("Error extracting data from source DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 3/4 (Connecting to destination DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to connect to destination DB...")
        print(f"Attmempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            dst_conn = init_dst_conn()
            print(25 * "=")
            print("Stage 3/4 successful")
            break
        except Exception as e:
            print("Error connecting to destination DB:", repr(e))
            time.sleep(3)

    print(25 * "=")
    print("Initiating stage 4/4 (Loading data into destination DB)")
    for i in range(1, max_attempts):
        print(25 * "=")
        print("Attempting to write data into destination DB...")
        print(f"Attmempt {i}/{max_attempts}")
        print(25 * "=")
        try:
            for df, table_name in extracted_data:
                load(dst_conn, df, table_name)
            print(25 * "=")
            print("Stage 4/4 successful")
            success = True
            break
        except Exception as e:
            print("Error loading data into destination DB:", repr(e))
            time.sleep(3)

    exit_code = 0 if success else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
