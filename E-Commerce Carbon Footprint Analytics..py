# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Data Ingestion
# MAGIC - ###   Data Reading
# MAGIC

# COMMAND ----------

file_path = "dbfs:/FileStore/shared_uploads/hari.priyaworks2005@gmail.com/ecommerce_carbon_footprint.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Load dataset

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/hari.priyaworks2005@gmail.com/ecommerce_carbon_footprint.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Show data

# COMMAND ----------

print("Data Loaded Successfully!")
df.show(10)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Data Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC - ###  Convert Spark DataFrame to Pandas for easy visualization

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Check for missing values

# COMMAND ----------

print("Missing values:\n", pdf.isnull().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Drop rows with nulls if any

# COMMAND ----------

pdf.dropna(inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC - ###  Verify cleaning

# COMMAND ----------

print("Data after cleaning:", pdf.shape)
pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Carbon Footprint Calculation

# COMMAND ----------

# MAGIC %md
# MAGIC - ###  Total carbon emissions
# MAGIC - ### Carbon Emission by Category
# MAGIC - ###  Carbon Emission by Delivery Mode
# MAGIC

# COMMAND ----------

total_emission = pdf['Carbon_Emission_kg'].sum()
print(f"Total Carbon Emission: {total_emission} kg")

# COMMAND ----------

category_emission = pdf.groupby('Category')['Carbon_Emission_kg'].sum().sort_values(ascending=False)
print("\nCarbon Emission by Category:\n", category_emission)

# COMMAND ----------

delivery_emission = pdf.groupby('DeliveryMode')['Carbon_Emission_kg'].sum()
print("\nCarbon Emission by Delivery Mode:\n", delivery_emission)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.Visualization using Matplotlib and pandas

# COMMAND ----------

# MAGIC %md
# MAGIC > ##  Bar chart – CO₂ emissions by delivery mode

# COMMAND ----------


import matplotlib.pyplot as plt
import pandas as pd

# Convert Spark DataFrame to Pandas
pdf = df.toPandas()

# Group by DeliveryMode for visualization
mode_summary = pdf.groupby("DeliveryMode")["Carbon_Emission_kg"].sum().reset_index()

# Plot a bar chart
plt.figure(figsize=(8, 5))
plt.bar(mode_summary["DeliveryMode"], mode_summary["Carbon_Emission_kg"], color=['skyblue', 'lightgreen', 'orange'])
plt.title("CO₂ Emissions by Delivery Mode", fontsize=14)
plt.xlabel("Delivery Mode", fontsize=12)
plt.ylabel("Total CO₂ Emissions (kg)", fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.6)

# Show the chart
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC > ## Pie chart –CO₂ emissions by product category

# COMMAND ----------

# Convert Carbon_Emission_kg to numeric
pdf["Carbon_Emission_kg"] = pd.to_numeric(pdf["Carbon_Emission_kg"], errors='coerce')
pdf = pdf.dropna(subset=["Carbon_Emission_kg"])
category_summary = pdf.groupby("Category")["Carbon_Emission_kg"].sum().reset_index()

plt.figure(figsize=(7, 7))
plt.pie(category_summary["Carbon_Emission_kg"], labels=category_summary["Category"], autopct='%1.1f%%', startangle=140)
plt.title("CO₂ Emissions by Product Category", fontsize=14)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ## Line chart – Emission trends over time

# COMMAND ----------

# Convert OrderDate to datetime
pdf["OrderDate"] = pd.to_datetime(pdf["OrderDate"])

# Group by OrderDate
date_summary = pdf.groupby("OrderDate")["Carbon_Emission_kg"].sum().reset_index()

plt.figure(figsize=(10, 5))
plt.plot(date_summary["OrderDate"], date_summary["Carbon_Emission_kg"], marker='o', color='green')
plt.title("Trend of CO₂ Emissions Over Time", fontsize=14)
plt.xlabel("Order Date", fontsize=12)
plt.ylabel("Total CO₂ Emissions (kg)", fontsize=12)
plt.grid(alpha=0.4)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ## Top 5 products – Ranked by emissions

# COMMAND ----------

top_products = pdf.groupby("ProductName")["Carbon_Emission_kg"].sum().reset_index().sort_values(by="Carbon_Emission_kg", ascending=False).head(5)

plt.figure(figsize=(8, 5))
plt.bar(top_products["ProductName"], top_products["Carbon_Emission_kg"], color='coral')
plt.title("Top 5 Products by CO₂ Emissions", fontsize=14)
plt.xlabel("Product Name", fontsize=12)
plt.ylabel("CO₂ Emissions (kg)", fontsize=12)
plt.xticks(rotation=30)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ### Stacked Bar: Category vs Delivery Mode

# COMMAND ----------

pivot_data = pdf.pivot_table(values="Carbon_Emission_kg", index="Category", columns="DeliveryMode", aggfunc="sum", fill_value=0)

pivot_data.plot(kind="bar", stacked=True, figsize=(10, 6))
plt.title("Emissions by Category and Delivery Mode", fontsize=14)
plt.xlabel("Category", fontsize=12)
plt.ylabel("CO₂ Emissions (kg)", fontsize=12)
plt.xticks(rotation=45)
plt.legend(title="Delivery Mode")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC > ### Histogram: Distribution of Emissions

# COMMAND ----------

plt.figure(figsize=(8, 5))
plt.hist(pdf["Carbon_Emission_kg"], bins=15, color='teal', alpha=0.7)
plt.title("Distribution of CO₂ Emissions per Order", fontsize=14)
plt.xlabel("CO₂ Emissions (kg)", fontsize=12)
plt.ylabel("Frequency", fontsize=12)
plt.grid(alpha=0.3)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC > ### Scatter Plot: Distance vs Emissions

# COMMAND ----------

plt.figure(figsize=(8, 5))
plt.scatter(pdf["Distance_km"], pdf["Carbon_Emission_kg"], color='orange', alpha=0.6)
plt.title("Distance vs CO₂ Emissions", fontsize=14)
plt.xlabel("Distance (km)", fontsize=12)
plt.ylabel("CO₂ Emissions (kg)", fontsize=12)
plt.grid(alpha=0.3)
plt.show()
