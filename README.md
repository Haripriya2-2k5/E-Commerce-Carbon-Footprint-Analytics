# E-Commerce Carbon Footprint Analytics

## 📌 Project Overview
This project analyzes carbon emissions from e-commerce orders based on product category, delivery mode, and distance. It is implemented using **Databricks Community Edition**, **PySpark**, and **Matplotlib**.

### ✨ Features
- Data ingestion from **DBFS**
- Data cleaning and transformation
- Carbon footprint calculations (CO₂ emissions)
- Visualizations:
  - Bar chart: Emissions by delivery mode
  - Pie chart: Emissions by category
  - Line chart: Emission trends over time
  - Stacked bar chart: Category vs Delivery Mode
  - Histogram: Emission distribution
  - Scatter plot: Distance vs Emissions

---

## 📂 Dataset
**File:** `ecommerce_carbon_footprint.csv`

**Columns:**
- `CustomerID`
- `ProductName`
- `Category`
- `Price`
- `Quantity`
- `OrderDate`
- `DeliveryMode` (Standard, Express, etc.)
- `Distance_km`
- `Carbon_Emission_kg`

---

## 🛠 How to Run This Project
1. Sign up for **Databricks Community Edition**: [https://community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Create a **cluster** (default settings)
3. Upload the dataset:
   - Go to **Data → Add Data → Upload File**
   - Place it in DBFS (`/FileStore/tables/`)
4. Upload the notebook:
   - Go to **Workspace → Import → Upload**
   - Choose the `carbon_footprint_analysis.py` file from `notebooks/`
5. Attach the notebook to the cluster and **run all cells**

---

## 📊 Visualizations
- **CO₂ Emissions by Delivery Mode** (Bar Chart)
- **CO₂ Emissions by Product Category** (Pie Chart)
- **Emission Trend Over Time** (Line Chart)
- **Top 5 Products by Emissions** (Bar Chart)
- **Emissions by Category & Delivery Mode** (Stacked Bar Chart)
- **Distribution of Emissions** (Histogram)
- **Distance vs Emissions** (Scatter Plot)

---

## ✅ Requirements
- **Databricks Runtime** (Community Edition)
- pandas
- matplotlib

---

## 📦 Install Locally (Optional)
```bash
pip install pandas matplotlib
