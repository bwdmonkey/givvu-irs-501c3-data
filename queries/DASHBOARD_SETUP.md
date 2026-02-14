# Looker Studio Dashboard Setup

Looker Studio (free) connects natively to BigQuery. No extra installs needed.

## Step 1: Create the dashboard tables

Run `04_dashboard_views.sql` in BigQuery console. This creates four pre-computed
tables optimized for fast dashboard loading:

| Table | Purpose | Rows |
|-------|---------|------|
| `dashboard_prospecting` | Main org list with all contact + donation data | ~22K |
| `dashboard_by_state` | State-level aggregates (for map chart) | ~52 |
| `dashboard_by_sector` | NTEE sector aggregates (for bar chart) | ~25 |
| `dashboard_donation_types` | Donation category breakdown (for pie chart) | ~7 |

## Step 2: Create a Looker Studio report

1. Go to **https://lookerstudio.google.com**
2. Click **"Create" → "Report"**
3. Click **"BigQuery"** as the data source
4. Select project `irs-dataset-487317` → dataset `irs_501c3_data_bq`
5. Choose table **`dashboard_prospecting`** → click **"Add"**
6. Click **"Add to report"** when prompted

## Step 3: Build the dashboard pages

### Page 1: Overview

Add these charts by clicking **"Add a chart"** in the toolbar:

| Chart Type | Data Source | Dimension | Metric |
|------------|-----------|-----------|--------|
| **Scorecard** | dashboard_prospecting | — | Record Count (COUNT) |
| **Scorecard** | dashboard_prospecting | — | SUM(total_noncash_amount) |
| **Scorecard** | dashboard_prospecting | — | SUM(food_amount) |
| **Geo Map** | dashboard_by_state | state | org_count or total_noncash |
| **Bar chart** | dashboard_by_sector | ntee_major_group | total_noncash |
| **Donut chart** | dashboard_donation_types | category | total_amount |

### Page 2: Prospecting List

| Chart Type | Data Source | Details |
|------------|-----------|---------|
| **Table** | dashboard_prospecting | Columns: org_name, city, state, org_phone, website, signing_officer_name, signing_officer_title, total_noncash_amount, food_amount, clothing_amount |

Add these **filter controls** (from "Add a control" menu):
- **Drop-down**: state
- **Drop-down**: ntee_major_group
- **Drop-down**: revenue_tier
- **Checkbox**: accepts_food
- **Checkbox**: accepts_clothing
- **Checkbox**: accepts_drugs_medical
- **Checkbox**: accepts_vehicles

This lets non-technical staff filter the list interactively.

### Page 3: State Detail

| Chart Type | Data Source | Details |
|------------|-----------|---------|
| **Geo Map** | dashboard_by_state | Dimension: state, Metric: total_noncash |
| **Table** | dashboard_by_state | All columns |
| **Stacked bar** | dashboard_by_state | Dimension: state, Metrics: food_total, clothing_total |

## Step 4: Add additional data sources

To use multiple tables in one report:
1. Click **"Resource" → "Manage added data sources"**
2. Click **"Add a data source"**
3. Select BigQuery → `dashboard_by_state` (and others)
4. Now you can pick which data source each chart uses

## Step 5: Share

- Click **"Share"** in the top right
- Add email addresses of your team
- Choose "Viewer" or "Editor" access
- They get a link to the live, interactive dashboard

## Refreshing the data

After re-running the pipeline:

```sql
-- Run 04_dashboard_views.sql again to refresh the materialized tables
-- Looker Studio will automatically show the updated data
```

## Alternative: BigQuery Studio notebooks

If you prefer a notebook experience inside BigQuery itself:

1. Go to BigQuery console
2. Click **"BigQuery Studio"** in the left nav (or "+ Create" → "Notebook")
3. Create a new Python notebook
4. You can run SQL cells directly:

```python
%%bigquery df
SELECT *
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE state = 'CA' AND accepts_food IS TRUE
LIMIT 50
```

5. Or use pandas for inline analysis:

```python
%%bigquery df
SELECT state, COUNT(*) as n, SUM(total_noncash_amount) as total
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
GROUP BY state ORDER BY total DESC

# Then in a Python cell:
df.plot.barh(x='state', y='total', figsize=(10, 12), title='Noncash Donations by State')
```

## Cost notes

- **Looker Studio**: Free
- **Dashboard tables**: The 4 tables total ~22K rows, using negligible storage (~5 MB)
- **Queries**: Each dashboard load scans these small tables, well within the 1 TB/month free tier
- **BigQuery Studio notebooks**: Free (included with BigQuery)
