## ! pip install plotly

import streamlit as st
import snowflake.connector
import pandas as pd
from snowflake.snowpark import Session
import plotly.express as px




sql_query = """SELECT * FROM CASE_STUDY.GOLD.EMP_ORDERS"""


try:
    conn = snowflake.connector.connect(**connection_parameters)
    cursor = conn.cursor()

    cursor.execute(sql_query)

    df = cursor.fetch_pandas_all()
    
    # --- 1. Display the Data Table ---
    st.header("Employee Sales Performance Sample Data")
    st.dataframe(df.head(5))


    # --- 2. Bar Chart: Total Units Sold by Employee ---
    # Replaces: Bar Chart: Count of Employees per Department
    st.header("Top Employee by Units Sold")
    
    # Sort by units sold and take the top 10 for a clean chart
    df_top_performers = df.sort_values(
        "EMPLOYEE_UNITS_SOLD", 
        ascending=False
    ).head(10)
    
    fig_bar = px.bar(
        df_top_performers,
        x="EMPLOYEE_NAME",
        y="EMPLOYEE_UNITS_SOLD",
        title="Top 10 Employees by Total Units Sold",
        labels={
            "EMPLOYEE_NAME": "Employee", 
            "EMPLOYEE_UNITS_SOLD": "Units Sold"
        }
    )
    st.plotly_chart(fig_bar, use_container_width=True)


    # --- 3. Pie Chart: Percentage Contribution of Each Employee ---
    # Replaces: Pie Chart: Percentage of Employees in Each Department
    st.header("Sales Contribution Breakdown by Employees")
    
    # Use the pre-calculated CONTRIBUTION_PERCENTAGE column
    # We'll filter out very small contributors to keep the pie chart clean
    df_pie_data = df[df['CONTRIBUTION_PERCENTAGE'] >= 2]
    
    fig_pie = px.pie(
        df_pie_data,
        values='CONTRIBUTION_PERCENTAGE',
        names='EMPLOYEE_NAME',
        title='Percentage Contribution to Total Sales (Employees with >2% Share)'
    )
    st.plotly_chart(fig_pie, use_container_width=True)


    # --- 4. Interactive Metrics Table ---
    st.header("Interactive Performance Table")
    
    # Setting a key metric (like ID or Employee_ID) as the index often looks cleaner
    st.dataframe(
        df.set_index('EMPLOYEE_ID'), 
        use_container_width=True
    )
    
        
except snowflake.connector.errors.DatabaseError as e:
    st.error(f"Error connecting to Snowflake: {e}")
    
finally:
    conn.close()
