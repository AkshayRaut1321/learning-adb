# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def addIngestionDateColumn(dataFrame):
    dataFrame.withColumn('ingestion_date', current_timestamp())
    return dataFrame;
