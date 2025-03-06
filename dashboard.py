import streamlit as st
import pandas as pd
import altair as alt
import duckdb

# Connect to DuckDB database
conn = duckdb.connect('database.duckdb')

# Load data using provided SQL query
query = """
select 
    s.fund_name,
    s.company_name,
    s.transaction_date,
    s.nav AS nav_static,
    s.ownership as ownership_static,
    d.nav AS nav_dynamic,
    d.ownership_pct as ownership_dynamic,
    (d.nav - s.nav) AS nav_delta,
    round(nav_delta/nav_static, 3) * 100 as percentual_difference
from company_nav as s
join company_nav_scaled as d on s.company_name = d.company_name 
                              and s.fund_name = d.fund_name
                              and s.transaction_date = d.transaction_date
order by 1, 2, 3
"""

df = conn.execute(query).fetchdf()
df['transaction_date'] = pd.to_datetime(df['transaction_date'])

# Sidebar controls
st.sidebar.header("Filters")
selected_fund = st.sidebar.selectbox(
    "Select Fund",
    options=df['fund_name'].unique(),
    index=0
)

# Filter companies based on selected fund
available_companies = df[df['fund_name'] == selected_fund]['company_name'].unique()
selected_company = st.sidebar.selectbox(
    "Select Company",
    options=available_companies,
    index=0
)

# Date range selector
date_range = st.sidebar.date_input(
    "Date Range",
    value=[df['transaction_date'].min(), df['transaction_date'].max()],
    min_value=df['transaction_date'].min(),
    max_value=df['transaction_date'].max()
)

# Filter data based on selections
filtered_df = df[
    (df['fund_name'] == selected_fund) &
    (df['company_name'] == selected_company) &
    (df['transaction_date'].between(
        pd.to_datetime(date_range[0]), 
        pd.to_datetime(date_range[1])
    ))
]

# Main content
st.title("NAV Calculation Method Comparison")
st.markdown(f"**Fund:** {selected_fund} | **Company:** {selected_company}")

# Custom date axis configuration
date_axis = alt.Axis(
    format="%Y-%m",  # Format as YYYY-MM
    labelAngle=-45,
    grid=False
)

# NAV Comparison Chart
st.header("NAV Comparison")
nav_chart = alt.Chart(filtered_df).transform_fold(
    fold=['nav_static', 'nav_dynamic'],
    as_=['calculation_type', 'nav']
).mark_line(point=True).encode(
    x=alt.X('transaction_date:T', title='Date', axis=date_axis),
    y=alt.Y('nav:Q', title='NAV Value', axis=alt.Axis(format='$,.0f')),
    color=alt.Color('calculation_type:N', 
                  legend=alt.Legend(title="Calculation Method"),
                  scale=alt.Scale(
                      domain=['nav_static', 'nav_dynamic'],
                      range=['#1f77b4', '#ff7f0e']
                  )),
    tooltip=[
        alt.Tooltip('transaction_date:T', title='Date', format='%Y-%m'),
        alt.Tooltip('calculation_type:N', title='Method'),
        alt.Tooltip('nav:Q', title='Value', format='$,.0f')
    ]
).properties(height=400)
st.altair_chart(nav_chart, use_container_width=True)

# Ownership Comparison Chart
st.header("Ownership Comparison")
ownership_chart = alt.Chart(filtered_df).transform_fold(
    fold=['ownership_static', 'ownership_dynamic'],
    as_=['ownership_type', 'percentage']
).mark_line(point=True).encode(
    x=alt.X('transaction_date:T', title='Date', axis=date_axis),
    y=alt.Y('percentage:Q', title='Ownership Percentage', axis=alt.Axis(format='.0%')),
    color=alt.Color('ownership_type:N', 
                  legend=alt.Legend(title="Ownership Type"),
                  scale=alt.Scale(
                      domain=['ownership_static', 'ownership_dynamic'],
                      range=['#2ca02c', '#d62728']
                  )),
    tooltip=[
        alt.Tooltip('transaction_date:T', title='Date', format='%Y-%m'),
        alt.Tooltip('ownership_type:N', title='Type'),
        alt.Tooltip('percentage:Q', title='Percentage', format='.2%')
    ]
).properties(height=400)
st.altair_chart(ownership_chart, use_container_width=True)

# Delta Analysis
st.header("Variance Analysis")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Absolute NAV Difference")
    st.altair_chart(alt.Chart(filtered_df).mark_bar().encode(
        x=alt.X('transaction_date:T', title='Date', axis=date_axis),
        y='nav_delta:Q',
        color=alt.condition(
            alt.datum.nav_delta > 0,
            alt.value('green'),
            alt.value('red')
        ),
        tooltip=[
            alt.Tooltip('transaction_date:T', title='Date', format='%Y-%m'),
            alt.Tooltip('nav_delta:Q', title='Difference', format='$,.0f')
        ]
    ), use_container_width=True)

with col2:
    st.subheader("Percentage Difference")
    st.altair_chart(alt.Chart(filtered_df).mark_line(point=True).encode(
        x=alt.X('transaction_date:T', title='Date', axis=date_axis),
        y=alt.Y('percentual_difference:Q', title='Difference (%)'),
        tooltip=[
            alt.Tooltip('transaction_date:T', title='Date', format='%Y-%m'),
            alt.Tooltip('percentual_difference:Q', title='Difference', format='.2f')
        ]
    ).properties(height=300), use_container_width=True)

# Data Table
st.header("Detailed Data View")
st.dataframe(
    filtered_df.sort_values('transaction_date').set_index('transaction_date')[
        ['nav_static', 'nav_dynamic', 'ownership_static', 'ownership_dynamic']
    ].style.format({
        'nav_static': '${:,.0f}',
        'nav_dynamic': '${:,.0f}',
        'ownership_static': '{:.2%}',
        'ownership_dynamic': '{:.2%}'
    }),
    use_container_width=True
)

conn.close()