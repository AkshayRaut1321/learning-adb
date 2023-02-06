# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def addIngestionDateColumn(dataFrame, columnName):
    return dataFrame.withColumn(columnName, current_timestamp())
