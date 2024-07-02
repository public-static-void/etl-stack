#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summary       : Perform ETL using Airflow.

Author        : Vadim Titov
Created       : Sun Jun 30 14:27:52 2024 +0200
Last modified : Tue Jul 02 18:55:08 2024 +0200
"""
import time
from datetime import datetime

import pandas as pd
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine

from airflow import settings

ETL_USER = Variable.get("ETL_USER")
ETL_PASS = Variable.get("ETL_PASS")
PG_PORT = Variable.get("PG_PORT")
SA_PORT = Variable.get("SA_PORT")
DRIVER = "{ODBC Driver 18 for SQL Server}"
SA_DB = "AdventureWorksDW2022"
PG_DB = "AdventureWorksDW2022AF"
SA_SERVER = "sqlserver"
PG_SERVER = "postgres"


@task()
def init_src_conn() -> dict[str, str]:
    """
    Create connection to the source DB.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    c = Connection(
        conn_id="sqlserver",
        conn_type="mssql",
        host=SA_SERVER,
        login=ETL_USER,
        password=ETL_PASS,
        schema=SA_DB,
        port=SA_PORT,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection)
        .filter(Connection.conn_id == c.conn_id)
        .first()
    )
    if str(conn_name) == str(c.conn_id):
        return {"message": "Connection already exists."}
    session.add(c)
    session.commit()
    return {"message": "success"}


@task()
def init_dst_conn() -> dict[str, str]:
    """
    Create connection to the destination DB.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    c = Connection(
        conn_id="postgres",
        conn_type="postgres",
        host=PG_SERVER,
        login=ETL_USER,
        password=ETL_PASS,
        schema=PG_DB,
        port=PG_PORT,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection)
        .filter(Connection.conn_id == c.conn_id)
        .first()
    )
    if str(conn_name) == str(c.conn_id):
        return {"message": "Connection already exists."}
    session.add(c)
    session.commit()
    return {"message": "success"}


@task()
def extract() -> dict[str, dict[str, str]]:
    """
    Extract data from source DB.

    Returns
    -------
    dict[str, dict[str, str]]
        Dictionary with table names as keys and corresponding table names as
        values.
    """
    src_conn = MsSqlHook(mssql_conn_id="sqlserver")
    query = """SELECT t.name
                AS table_name
                FROM sys.tables t
                WHERE t.name
                IN ('DimProduct',
                    'DimProductCategory',
                    'DimProductSubcategory'
                    )"""
    df = src_conn.get_pandas_df(query)
    tbl_dict = df.to_dict("dict")
    return tbl_dict


@task()
def prepare_transform(
    tbl_dict: dict,
) -> list[str]:
    """
    Create tables in destination DB.

    Parameters
    ----------
    tbl_dict : dict
        Dictionary with table names as keys and corresponding table names as
        values.

    Returns
    -------
    list[str]
        List of table names.
    """
    src_conn = MsSqlHook(mssql_conn_id="sqlserver")
    conn = BaseHook.get_connection("postgres")
    dst_conn = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    table_names = []
    start_time = time.time()
    for _, table_name in tbl_dict["table_name"].items():
        table_names.append(table_name)
        rows_imported = 0
        query = f"SELECT * FROM {table_name}"
        df = src_conn.get_pandas_df(query)
        print(
            f"Importing rows {rows_imported} to {rows_imported + len(df)}..."
            f" for table {table_name}."
        )
        df.to_sql(
            f"src_{table_name}", dst_conn, if_exists="replace", index=False
        )
        rows_imported += len(df)
        print(
            f"Done. {str(round(time.time() - start_time, 2))} total seconds"
            " elapsed."
        )
    print("Data imported successfully.")
    return table_names


@task()
def transform_product() -> dict[str, str]:
    """
    Transform product data.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    conn = BaseHook.get_connection("postgres")
    dst_conn = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    df = pd.read_sql_query('SELECT * FROM public."src_DimProduct" ', dst_conn)
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
    df.to_sql(f"stg_DimProduct", dst_conn, if_exists="replace", index=False)
    return {"table(s) processed ": "Data imported successful"}


@task()
def transform_product_subcategory() -> dict[str, str]:
    """
    Transform Product Subcategory.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    conn = BaseHook.get_connection("postgres")
    dst_conn = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    df = pd.read_sql_query(
        'SELECT * FROM public."src_DimProductSubcategory" ', dst_conn
    )
    df = df[
        [
            "ProductSubcategoryKey",
            "EnglishProductSubcategoryName",
            "ProductSubcategoryAlternateKey",
            "ProductCategoryKey",
        ]
    ]
    df = df.rename(
        columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"}
    )
    df.to_sql(
        f"stg_DimProductSubcategory",
        dst_conn,
        if_exists="replace",
        index=False,
    )
    return {"table(s) processed ": "Data imported successful"}


@task()
def transform_product_category() -> dict[str, str]:
    """
    Transform Product Category.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    conn = BaseHook.get_connection("postgres")
    dst_conn = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    df = pd.read_sql_query(
        'SELECT * FROM public."src_DimProductCategory" ', dst_conn
    )
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
    df.to_sql(
        f"stg_DimProductCategory", dst_conn, if_exists="replace", index=False
    )
    return {"table(s) processed ": "Data imported successful"}


@task()
def merge_tables() -> dict[str, str]:
    """
    Merge tables for product, product subcategory and product category.

    Returns
    -------
    dict[str, str]
        Success message.
    """
    conn = BaseHook.get_connection("postgres")
    dst_conn = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    pc = pd.read_sql_query(
        'SELECT * FROM public."stg_DimProductCategory" ', dst_conn
    )
    p = pd.read_sql_query('SELECT * FROM public."stg_DimProduct" ', dst_conn)
    ps = pd.read_sql_query(
        'SELECT * FROM public."stg_DimProductSubcategory" ', dst_conn
    )
    merged = p.merge(ps, on="ProductSubcategoryKey").merge(
        pc, on="ProductCategoryKey"
    )
    merged.to_sql(
        f"prd_DimProductCategory", dst_conn, if_exists="replace", index=False
    )
    return {"table(s) processed ": "Data imported successful"}


with DAG(
    dag_id="product_ETL_dag",
    schedule_interval="0 9 * * *",
    start_date=datetime(2022, 3, 5),
    catchup=False,
    tags=["product_model"],
) as dag:
    with TaskGroup("init_connections", tooltip="Init") as init_group:
        src_init = init_src_conn()
        dst_init = init_dst_conn()
        [src_init, dst_init]

    with TaskGroup(
        "extract_product", tooltip="Extract data from source DB."
    ) as extract_group:
        tbl_dict = extract()
        tbl_dict

    with TaskGroup(
        "transform_product", tooltip="Transform data."
    ) as transform_group:
        table_names = prepare_transform(tbl_dict)
        transform_product = transform_product()
        transform_product_subcategory = transform_product_subcategory()
        transform_product_category = transform_product_category()
        (
            table_names
            >> [
                transform_product,
                transform_product_subcategory,
                transform_product_category,
            ]
        )

    with TaskGroup(
        "load_product", tooltip="Load transformed data into destination DB."
    ) as load_group:
        merge_tables = merge_tables()
        merge_tables

    init_group >> extract_group >> transform_group >> load_group

if __name__ == "__main__":
    dag.test()
