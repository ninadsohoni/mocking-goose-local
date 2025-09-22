---
name: build_lakeview_dashboard
description: Build comprehensive Lakeview Dashboard from Unity Catalog tables
arguments:
  - name: catalog
    description: Unity Catalog name containing source data
    required: false
    schema:
      type: string
      pattern: "^[a-zA-Z][a-zA-Z0-9_]*$"
  
  - name: schema
    description: Schema containing tables (use all tables) - mutually exclusive with table_names
    required: false
    schema:
      type: string
      pattern: "^[a-zA-Z][a-zA-Z0-9_]*$"
  
  - name: table_names
    description: Specific table names (catalog.schema.table format) - mutually exclusive with schema
    required: false
    schema:
      type: array
      items:
        type: string
        pattern: "^[a-zA-Z][a-zA-Z0-9_]*\\.[a-zA-Z][a-zA-Z0-9_]*\\.[a-zA-Z][a-zA-Z0-9_]*$"
      minItems: 1
      maxItems: 50
  
  - name: warehouse_id
    description: SQL Warehouse ID for query execution
    required: true
    schema:
      type: string
      pattern: "^[a-f0-9]{16}$"
  
  - name: dashboard_name
    description: Name for the dashboard
    required: false
    schema:
      type: string
      maxLength: 255
  

mutually_exclusive:
  - [schema, table_names]
---

Build a Lakeview Dashboard from tables in Databricks with optimized widgets, layouts, and production-ready deployment.

## Context

**Configuration Provided:**
- Warehouse ID: {warehouse_id}
- Catalog: {catalog}
- Schema: {schema}
- Tables: {table_names}
- Dashboard Name: {dashboard_name}

## Objective

Create production-ready Lakeview Dashborads by:
1. Discovering and analyzing the data structure
2. Creating optimized SQL datasets with widget expressions
3. Building responsive dashboard layouts with appropriate visualizations
4. Deploying via Databricks Asset Bundles

## Workflow

### 1: Validation & Discovery (REQUIRED FIRST)
- Make sure to get the values of the parameter from the user before running any tool.
- **STOP**: Verify workspace context and required parameters
- **Authentication**: Uses DATABRICKS_TOKEN environment variable for authentication
- Validate source table accessibility
- Understand business context and key metrics to highlight
- Identify relationships between tables
- Extract configuration from existing databricks.yml if present
- Identify table relationships and data patterns

### 2. Query Design & Validation
- **ALWAYS** Use widget-level aggregations rather than pre-aggregated datasets
- Design consolidated datasets that support multiple widgets
- Test all SQL queries with `execute_dbsql` before widget creation
- Validate column names, data types, and handle edge cases
- Design consolidated datasets supporting multiple widgets (avoid one dataset per widget)
- Implement robust SQL with COALESCE, CASE statements for NULL safety, division by zero prevention
- Use LEFT JOINs to handle missing dimension data gracefully

### 3. Dashboard Creation Strategy

**Critical Dashboard Requirements:**
- Use optimized datasets with widget expressions for flexibility
- Implement responsive grid positioning (12-column system)
- Include variety of widget types: counters, charts, tables, heatmaps
- Add descriptive titles, descriptions and formatting for all widgets
- Handle missing data scenarios gracefully

**Dataset Design Principles:**
- One dataset per logical entity (sales, customers, orders)
- Include raw dimensions for filtering and grouping
- Impement widget level aggregations through expressions over aggregations in Datasets

**Widget Expression Patterns:**
```sql
-- Aggregations in widgets, not datasets
y_expression: "SUM(revenue)"
x_expression: "DATE_TRUNC('MONTH', date)"

-- Conditional counts
"COUNT(CASE WHEN status = 'active' THEN 1 END)"

-- Percentages with safe division
"CASE WHEN SUM(total) > 0 THEN SUM(value)/SUM(total) * 100 ELSE 0 END"
```
- Optimize for performance with proper indexing hints

### 4. Dashboard Implementation
- Create dashboard using `create_dashboard_file` with validated configurations
- Design 12-column responsive grid layout
- Position KPIs at top for immediate visibility
- Add supporting charts with logical flow from overview to detail
- Include interactive filters for user exploration

**Layout Guidelines:**
- Full width: `width: 12` (for headers/separators)
- Half width: `width: 6` (side-by-side comparisons)
- Quarter width: `width: 3` (KPI cards)
- Standard height: `height: 4` (most widgets)

### 5. Deployment & Validation

- Deploys via Databricks Asset Bundles with serverless compute
- Create Databricks Asset Bundle structure
- Generate `databricks.yml` with proper configurations
- Deploy using `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle deploy`
- Monitor dashboard rendering and fix any issues
- Validate all widgets display correctly

### 6. Asset Bundle Configuration

**Critical Configuration Requirements:**
- Use `file_path` (not `serialized_dashboard`) for native dashboard resources
- Include sync exclusion to prevent duplicate dashboards:
  ```yaml
  sync:
    exclude:
      - "*.lvdash.json"
  ```
- Include proper `root_path` configuration to avoid warnings
- Use correct permission levels for dashboards (`CAN_READ`, `CAN_MANAGE`)
- Remove unsupported fields from databricks.yml (exclude/include patterns not supported in current CLI version)

**Example databricks.yml Configuration:**
```yaml
bundle:
  name: my_dashboard_bundle

workspace:
  root_path: /Workspace/Users/${workspace.current_user.userName}/dashboards

sync:
  exclude:
    - "*.lvdash.json"

resources:
  dashboards:
    my_dashboard:
      display_name: "Sales Analytics Dashboard"
      file_path: ./src/dashboard.lvdash.json
      permissions:
        - level: CAN_MANAGE
          user_name: ${workspace.current_user.userName}
        - level: CAN_READ
          group_name: analysts

targets:
  dev:
    workspace:
      host: ${DATABRICKS_HOST}
```
### 7. Automated Deployment & Validation
- Run `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle validate` before deployment
- Execute `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle deploy --target dev` 
- Provide `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle summary` output
- Include direct dashboard URL for immediate access
- Handle deployment errors gracefully with troubleshooting steps

## Best Practices

### Widget Selection Guide
- **Counters**: Single KPI metrics - **REQUIRES explicit `value_field` with aggregation** (see detailed requirements below)
- **Bar Charts**: Categorical comparisons
- **Line Charts**: Time series trends
- **Tables**: Detailed data exploration
- **Pie Charts**: Part-to-whole relationships
- **Heatmaps**: Two-dimensional analysis

### ⚠️ CRITICAL: Counter Widget Configuration Requirements

**Counter widgets are BLANK if `value_field` is not properly specified.** You MUST explicitly provide the aggregation function and field name:

### **Required Counter Widget Structure:**
```json
{
  "type": "counter",
  "dataset": "Dataset Name", 
  "config": {
    "value_field": "AGGREGATION(field_name)",    // ⚠️ REQUIRED - specify aggregation
    "value_display_name": "Display Name",        // Optional: Custom display name  
    "title": "Widget Title",                     // Widget title
    "show_title": true
  }
}
```

### **Counter Widget Examples by Business Intent:**

#### **📊 COUNT Widgets (Total Records)**
```json
// Count all records
{"value_field": "count(*)", "title": "Total Records"}

// Count specific entity (recommended)
{"value_field": "count(customer_id)", "title": "Total Customers"}
{"value_field": "count(order_id)", "title": "Total Orders"}
{"value_field": "count(lease_id)", "title": "Total Leases"}

// Count distinct values
{"value_field": "count(distinct customer_id)", "title": "Unique Customers"}
```

#### **💰 SUM Widgets (Financial Totals)**
```json
// Revenue/Income totals
{"value_field": "sum(revenue)", "title": "Total Revenue"}
{"value_field": "sum(monthly_rent)", "title": "Monthly Rent Total"}
{"value_field": "sum(order_amount)", "title": "Total Sales"}

// Quantity totals
{"value_field": "sum(quantity)", "title": "Total Units Sold"}
{"value_field": "sum(units_produced)", "title": "Production Volume"}
```

#### **📈 AVERAGE Widgets (Metrics)**
```json
// Financial averages
{"value_field": "avg(order_amount)", "title": "Average Order Value"}
{"value_field": "avg(monthly_rent)", "title": "Average Monthly Rent"}
{"value_field": "avg(salary)", "title": "Average Salary"}

// Performance averages
{"value_field": "avg(rating)", "title": "Average Rating"}
{"value_field": "avg(satisfaction_score)", "title": "Avg Satisfaction"}
{"value_field": "avg(square_feet)", "title": "Average Property Size"}
```

### **🎯 Dashboard Planning Guide**

**Before creating counter widgets, identify:**

1. **What do you want to COUNT?** → Use `count(id_field)` or `count(*)`
2. **What do you want to SUM?** → Use `sum(amount_field)` 
3. **What do you want to AVERAGE?** → Use `avg(metric_field)`

**Your business context determines the right aggregation:**
- **E-commerce**: `count(order_id)`, `sum(revenue)`, `avg(order_value)`
- **Healthcare**: `count(patient_id)`, `sum(treatment_cost)`, `avg(satisfaction)`
- **Finance**: `count(transaction_id)`, `sum(amount)`, `avg(balance)`
- **Education**: `count(student_id)`, `sum(tuition)`, `avg(gpa)`
- **Real Estate**: `count(lease_id)`, `sum(monthly_rent)`, `avg(square_feet)`

**⚠️ Missing `value_field` = Blank Widget!** The system will show a fallback `count(*)` but warn you to specify the correct aggregation.

### Error Prevention
- Verify table existence before querying
- Check column data types match widget requirements
- Test with sample data before full deployment
- Include error handling in SQL queries

## Available Tools

**Data Exploration:**
- `list_uc_schemas`, `list_uc_tables`
- `describe_uc_catalog`, `describe_uc_schema`, `describe_uc_table`
- `execute_dbsql` - Test and validate queries

**Dashboard Management:**
- `create_dashboard_file` - Create new dashboard with widgets
- `validate_dashboard_sql` - Validate SQL before dashboard creation
- `get_widget_configuration_guide` - Widget configuration reference

## Success Criteria

✓ All SQL queries execute without errors
✓ Dashboard renders with all widgets displaying data
✓ Asset Bundle deploys successfully
✓ Performance meets expectations (<3s load time)
✓ **Bundle Validation**: `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle validate` passes without errors  
✓ **Successful Deployment**: `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle deploy --target dev` completes successfully  
✓ **Resource Creation**: Dashboard appears in `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle summary --target dev` output  
✓ **Direct Access**: Dashboard URL is accessible and opens in browser via `DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_TOKEN=$DATABRICKS_TOKEN databricks bundle open`  
✓ **Data Safety**: No SQL errors due to NULL values or missing data 
✓ **Join Integrity**: LEFT JOINs prevent data loss when dimension tables are incomplete  
✓ **Widget Field Expression**: Widget level aggregations (SUM(), COUNT(DISTINCT `field_name`) are used

## Example Dashboard Structure

```yaml
Dashboard:
  - Row 1: KPI Cards (4 counters)
  - Row 2: Revenue Trend (line chart) | Category Breakdown (bar chart)
  - Row 3: Detailed Table with Filters
  - Row 4: Geographic Distribution (map) | Top Products (horizontal bar)
```

## Notes

- Prioritize widget expressions over pre-aggregated datasets for flexibility
- Use parameterized queries for dynamic filtering
- Consider creating multiple dashboards for different user personas
- Document assumptions and data refresh schedules

Ready to build your Lakeview Dashboard! Provide any additional requirements or context to customize the implementation.