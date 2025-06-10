import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT   #for auto-comitting of values

# Function to connect to the PostgreSQL database
def connection():
    try:
        conn = psycopg2.connect(    #connection statement for posgres to VS Code
            host="localhost",
            user="postgres",
            password="934446",
            database="policepostregister",
            port=5432
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)    #for auto-comitting of values
        return conn
    except Exception as e:    # exception handling
        st.error(f"Database connection error: {e}")
        return None

# Function to fetch data and return a DataFrame with column names
def fetchdata(query):
    connect_with = connection()
    if connect_with:
        try:
            cursor = connect_with.cursor()    # making connection with query
            cursor.execute(query)      # for executing the query
            rows = cursor.fetchall()    # fetching all the datas from 
            columns = [desc[0] for desc in cursor.description]  # to get column names
            df = pd.DataFrame(rows, columns=columns)
            return df
        except Exception as e:      # exception handling
            st.error(f"Query execution error: {e}")
            return pd.DataFrame()
        finally:
            connect_with.close()    # for closing the connection after execution of query and fetching all the data
    else:
        return pd.DataFrame()

# Streamlit app visuals
st.set_page_config(page_title="Securecheck Police Dashboard", layout="wide")
st.title("🚓Secure Check Police Post Log🚨")
st.markdown("Real-time tracking of checkpost ledger")
st.header("Policelogs Overview")

# Initial preview query of table of log data
query = """SELECT * FROM "Policelog";"""
data = fetchdata(query)
st.dataframe(data, use_container_width=True)

#For creating chart tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Country","Violation","Stop Outcome","Arrested or not","Drug Related Stops"
])

with tab1:
    if not data.empty and "country_name" in data.columns:
        country_chart = data["country_name"].value_counts().nlargest(5).reset_index()
        country_chart.columns = ["Country Name", "Count"]

        Chart = px.bar(
            country_chart,
            x="Country Name",
            y="Count",
            title="Top Countries",
            color="Country Name",
            width=500,
            height=500
        )
        st.plotly_chart(Chart)
    else:
        st.warning("No data found or 'country_name' column not found.")

with tab2:
    if not data.empty and "violation" in data.columns:
        violation_chart = data["violation"].value_counts().nlargest(5).reset_index()
        violation_chart.columns = ["Violation", "Count"]

        Chart = px.bar(
            violation_chart,
            x="Violation",
            y="Count",
            title="Top Violations",
            color="Violation",
            width=500,
            height=500
        )
        st.plotly_chart(Chart)
    else:
        st.warning("No data found or 'violation' column not found.")

with tab3:
    if not data.empty and "stop_outcome" in data.columns:
        outcome_chart = data["stop_outcome"].value_counts().nlargest(5).reset_index()
        outcome_chart.columns = ["Stop outcome", "Count"]

        Chart = px.bar(
            outcome_chart,
            x="Stop outcome",
            y="Count",
            title="Stop Outcome",
            color="Stop outcome",
            width=500,
            height=500
        )
        st.plotly_chart(Chart)
    else:
        st.warning("No data found or 'stop_outcome' column not found.")

with tab4:
    if not data.empty and "is_arrested" in data.columns:
        arrest_chart = data["is_arrested"].value_counts().reset_index()
        arrest_chart.columns = ["Arrested", "Count"]

        Chart = px.bar(
            arrest_chart,
            x="Arrested",
            y="Count",
            title="Arrested",
            color="Arrested",
            width=500,
            height=500
        )
        st.plotly_chart(Chart)
    else:
        st.warning("No data found or 'is_arrested' column not found.")

with tab5:
    if not data.empty and "drugs_related_stop" in data.columns:
        stop_chart = data["drugs_related_stop"].value_counts().reset_index()
        stop_chart.columns = ["Is Drug Related", "Count"]

        chart = px.bar(
            stop_chart,
            x="Is Drug Related",
            y="Count",
            title="Drugs Related Stops",
            color="Is Drug Related",
            width=500,
            height=500
        )
        st.plotly_chart(chart)
    else:
        st.warning("No data found or 'drugs_related_stop' column not found.")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stops = data.shape[0]
    st.metric("Total Police Stops", total_stops)

with col2:
    total_arrest = data[data["stop_outcome"].str.contains("arrest", case=False, na=False)].shape[0]
    st.metric("Total Arrests", total_arrest)

with col3:
    total_warning = data[data["stop_outcome"].str.contains("warning", case=False, na=False)].shape[0]
    st.metric("Total Warning", total_warning)

with col4:
    Drug_related = data[data["drugs_related_stop"]==1].shape[0]
    st.metric("Drug Related Stop", Drug_related)

# Creating dropdown for queries
st.header("Project Queries")
select_query = st.selectbox("Select query to run", [
    "Top 10 vehicle_Number involved in drug-related stops",
    "Most frequently searched vehicles",
    "Highest arrest rate according to driver age group",
    "Gender distribution of drivers stopped in each country",
    "Race and gender combination having the highest search rate",
    "Time of day sees the most traffic stops",
    "Average stop duration for different violations",
    "Are stops during the night more likely to lead to arrests",
    "Violations that are most associated with searches or arrests",
    "Violations are most common among younger drivers (<25)",
    "Violation that rarely results in search or arrest",
    "Countries report the highest rate of drug-related stop",
    "Arrest rate by country and violation",
    "Country having the most stops with search conducted",
    "Yearly Breakdown of Stops and Arrests by Country",
    "Driver Violation Trends Based on Age and Race",
    "Time Period Analysis of Stops, Number of Stops by Year,Month, Hour of the Day",
    "Violations with High Search and Arrest Rates",
    "Driver Demographics by Country",
    "Top 5 Violations with Highest Arrest Rates"
])

# Mapping queries with select_query
query_map = {
    "Top 10 vehicle_Number involved in drug-related stops": """select vehicle_number, count(*) as drug_stop_count from "Policelog" 
    where drugs_related_stop = true and vehicle_number is not null 
    group by vehicle_number 
    order by drug_stop_count desc limit 10;""",
    "Most frequently searched vehicles": """select vehicle_number, count(*) as search_count from "Policelog" 
    where search_conducted = true and vehicle_number is not null 
    group by vehicle_number 
    order by search_count desc limit 10;""",
    "Highest arrest rate according to driver age group": """select age_group, count (*) as total_stops, 
    sum(case when is_arrested = true then 1 else 0 end) as total_arrests, 
    round(100.0*sum(case when is_arrested = true then 1 else 0 end)/count(*),2) as arrest_rate 
    from (select *,case 
    when driver_age<18 then 'under 18'
    when driver_age between 18 and 25 then '18-25'
    when driver_age between 26 and 40 then '26-40'
    when driver_age between 41 and 60 then '41-60'
    when driver_age between 61 and 80 then '61-80'
    else 'unknown' end as age_group
    from "Policelog"
    where driver_age is not null
    )as grouped_data group by age_group 
    order by arrest_rate desc limit 1;""",
    "Gender distribution of drivers stopped in each country": """select country_name,
    sum(case when driver_gender = 'M' then 1 else 0 end) as Male_count,
    sum(case when driver_gender = 'F' then 1 else 0 end) as Female_count from "Policelog" 
    where driver_gender is not null group by country_name""",
    "Race and gender combination having the highest search rate": """SELECT driver_race, driver_gender,COUNT(*) AS total_stops, 
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
    ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*),2) AS search_rate_percent
    FROM Policelog WHERE driver_race IS NOT NULL AND driver_gender IS NOT NULL
    GROUP BY driver_race, driver_gender 
    ORDER BY search_rate_percent DESC LIMIT 1;""",
    "Time of day sees the most traffic stops": """SELECT EXTRACT(HOUR FROM timestamp) AS stop_hour,
    COUNT(*) AS total_stops FROM "Policelog"
    WHERE timestamp IS NOT NULL
    GROUP BY stop_hour ORDER BY total_stops DESC LIMIT 1;""",
    "Average stop duration for different violations": """SELECT violation, ROUND(AVG(CASE 
    WHEN stop_duration = '0-15 Min' THEN 7.5
    WHEN stop_duration = '16-30 Min' THEN 23
    WHEN stop_duration = '30+ Min' THEN 35
    ELSE NULL END), 2) AS avg_stop_duration_minutes
    FROM "Policelog"
    WHERE stop_duration IS NOT NULL AND violation IS NOT NULL
    GROUP BY violation ORDER BY avg_stop_duration_minutes DESC;""",
    "Are stops during the night more likely to lead to arrests": """WITH time_classified AS (SELECT *,
    CASE WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 6 AND 17 THEN 'Day'
    ELSE 'Night' END AS time_of_day
    FROM "Policelog" WHERE timestamp IS NOT NULL),
    arrest_summary AS (SELECT time_of_day,COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate_percent
    FROM time_classified GROUP BY time_of_day)
    SELECT * FROM arrest_summary
    ORDER BY time_of_day;
    ""","Violations that are most associated with searches or arrests": """SELECT violation, COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
    ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS search_rate_percent,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate_percent
    FROM "Policelog" WHERE violation IS NOT NULL GROUP BY violation
    ORDER BY total_searches DESC, total_arrests DESC;""",
    "Violations are most common among younger drivers (<25)": """SELECT violation,COUNT(*) AS stop_count FROM Policelog
    WHERE driver_age < 25 AND violation IS NOT NULL GROUP BY violation ORDER BY stop_count DESC;""",
    "Violation that rarely results in search or arrest": """SELECT violation,COUNT(*) AS total_stops,
    COUNT(CASE WHEN search_conducted = TRUE OR is_arrested = TRUE THEN 1 END) AS stops_with_search_or_arrest,
    (CAST(COUNT(CASE WHEN search_conducted = TRUE OR is_arrested = TRUE THEN 1 END) AS NUMERIC) * 100.0 / COUNT(*)) AS search_or_arrest_percentage
    FROM Policelog GROUP BY violation
    HAVING COUNT(*) > 0
    ORDER BY search_or_arrest_percentage ASC,
    total_stops DESC LIMIT 1;""",
    "Countries report the highest rate of drug-related stop": """SELECT country_name,
    COUNT(*) AS total_stops,
    COUNT(CASE WHEN drugs_related_stop = TRUE THEN 1 END) AS drug_related_stops,
    (CAST(COUNT(CASE WHEN drugs_related_stop = TRUE THEN 1 END) AS NUMERIC) * 100.0 / COUNT(*)) AS drug_related_percentage
    FROM Policelog
    WHERE country_name IS NOT NULL
    GROUP BY country_name
    HAVING COUNT(*) > 0
    ORDER BY drug_related_percentage DESC limit 1;""",
    "Arrest rate by country and violation": """SELECT country_name,violation,COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate_percent
    FROM "Policelog" WHERE country_name IS NOT NULL AND violation IS NOT NULL
    GROUP BY country_name, violation
    ORDER BY arrest_rate_percent DESC;""",
    "Country having the most stops with search conducted": """SELECT country_name,COUNT(*) AS total_stops_with_search
    FROM "Policelog"
    WHERE search_conducted = TRUE AND country_name IS NOT NULL
    GROUP BY country_name
    ORDER BY total_stops_with_search DESC
    LIMIT 1;""",
    "Yearly Breakdown of Stops and Arrests by Country": """SELECT year,country_name,total_stops,total_arrests,
    ROUND(100.0 * total_arrests / total_stops, 2) AS arrest_rate_percent,
    RANK() OVER (PARTITION BY year ORDER BY total_arrests DESC) AS country_rank_by_arrests
    FROM (SELECT EXTRACT(YEAR FROM timestamp) AS year,
        country_name,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
    FROM "Policelog"
    WHERE timestamp IS NOT NULL AND country_name IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM timestamp), country_name
    ) AS yearly_stats
    ORDER BY year, country_rank_by_arrests;""",
    "Driver Violation Trends Based on Age and Race": """WITH age_grouped AS (SELECT driver_race,
    CASE
	WHEN driver_age IS NULL THEN 'Unknown'
    WHEN driver_age < 18 THEN 'Under 18'
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    WHEN driver_age > 60 THEN '60+'
    ELSE 'Unknown' END AS age_group,
    violation,COUNT(*) AS violation_count FROM "Policelog"
    WHERE driver_race IS NOT NULL AND violation IS NOT NULL
    GROUP BY driver_race, age_group, violation),
    top_violations AS (SELECT driver_race,age_group,violation,violation_count,
    RANK() OVER (PARTITION BY driver_race, age_group ORDER BY violation_count DESC) AS rank
    FROM age_grouped)
    SELECT 
    t.driver_race,
    t.age_group,
    t.violation,
    t.violation_count,
    totals.total_stops
    FROM top_violations t
    JOIN (
    SELECT driver_race,
    CASE
    WHEN driver_age IS NULL THEN 'Unknown'
    WHEN driver_age < 18 THEN 'Under 18'
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    WHEN driver_age > 60 THEN '60+'
    ELSE 'Unknown'
    END AS age_group,
    COUNT(*) AS total_stops
    FROM "Policelog"
    WHERE driver_race IS NOT NULL
    GROUP BY driver_race, age_group
    ) totals
    ON t.driver_race = totals.driver_race AND t.age_group = totals.age_group
    WHERE t.rank = 1
    ORDER BY t.driver_race, t.age_group;""",
    "Time Period Analysis of Stops, Number of Stops by Year,Month, Hour of the Day": """SELECT
    EXTRACT(YEAR FROM timestamp) AS stop_year,
    EXTRACT(MONTH FROM timestamp) AS stop_month,
    EXTRACT(HOUR FROM timestamp) AS stop_hour,
    COUNT(*) AS total_stops
    FROM "Policelog"
    WHERE timestamp IS NOT NULL
    GROUP BY stop_year, stop_month, stop_hour
    ORDER BY stop_year, stop_month, stop_hour;""",
    "Violations with High Search and Arrest Rates": """WITH violation_stats AS (
    SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS search_count,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_count,
        ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS search_rate_percent,
        ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate_percent
    FROM "Policelog" WHERE violation IS NOT NULL GROUP BY violation),
    ranked_violations AS (
    SELECT *,RANK() OVER (ORDER BY search_rate_percent DESC) AS search_rank,
    RANK() OVER (ORDER BY arrest_rate_percent DESC) AS arrest_rank
    FROM violation_stats)
    SELECT violation,total_stops,search_count,search_rate_percent,search_rank,arrest_count,arrest_rate_percent,arrest_rank
    FROM ranked_violations
    WHERE search_rank <= 5 OR arrest_rank <= 5
    ORDER BY search_rank, arrest_rank;""",
    "Driver Demographics by Country": """WITH driver_data AS (
    SELECT country_name,driver_gender,driver_race,
    CASE
    WHEN driver_age IS NULL THEN 'Unknown'
    WHEN driver_age < 18 THEN 'Under 18'
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    WHEN driver_age > 60 THEN '60+'
    ELSE 'Unknown' END AS age_group
    FROM "Policelog"
    WHERE country_name IS NOT NULL
        AND driver_gender IS NOT NULL
        AND driver_race IS NOT NULL
        AND driver_age IS NOT NULL)
    SELECT country_name,age_group,driver_gender,driver_race,
    COUNT(*) AS total_stops
    FROM driver_data
    GROUP BY country_name, age_group, driver_gender, driver_race
    ORDER BY country_name, age_group, driver_gender, driver_race;""",
    "Top 5 Violations with Highest Arrest Rates": """WITH violation_stats AS (
    SELECT violation,COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate_percent
    FROM "Policelog" WHERE violation IS NOT NULL
    GROUP BY violation)
    SELECT violation,total_stops,total_arrests,arrest_rate_percent
    FROM violation_stats
    ORDER BY arrest_rate_percent DESC
    LIMIT 5;"""
}

# Button to run selected query
if st.button("Run Query"):
    result = fetchdata(query_map[select_query])
    if not result.empty:
        st.dataframe(result, use_container_width=True)
    else:
        st.warning("No results found for the selected query.")


st.markdown("---")
st.markdown("Built with ❤️ for law Enforcement by Securecheck")
st.header("🔍 Custom Natural Language filter")


st.markdown("Fill in the details below to get the Natural language prediction of the stop outcome based on existing data")

st.header("📖 Add new police log & Predict outcome and violation")

unique_durations = data["stop_duration"].dropna().unique()

with st.form("New Log Form"):
    stop_date = st.date_input("Stop Date")
    stop_time = st.time_input("Stop Time")
    country_name = st.text_input("Country Name")
    driver_gender = st.selectbox("Driver Gender", ["M", "F"])
    driver_age = st.number_input("Driver Age", min_value=0, max_value=100)
    driver_race = st.text_input("Driver Race")
    search_conducted = st.selectbox("Was a Search Conducted", [True, False])
    search_type = st.text_input("Search Type")
    drugs_related_stop = st.selectbox("Was it a drug-related stop", [True, False])
    stop_duration = st.selectbox("Stop Duration", unique_durations)
    vehicle_number = st.text_input("Vehicle Number")
    timestamp = pd.Timestamp.now()

    submitted = st.form_submit_button("Predict the Outcome")

    if submitted:
        # Filter based on selected inputs
        filtered_data = data[
            (data['driver_gender'] == driver_gender) &
            (data['driver_age'] == driver_age) &
            (data['search_conducted'] == search_conducted) &
            (data['stop_duration'] == stop_duration) &
            (data['drugs_related_stop'] == drugs_related_stop)
        ]

        if not filtered_data.empty:
            predicted_outcome = filtered_data['stop_outcome'].mode()[0]
            predicted_violation = filtered_data['violation'].mode()[0]
        else:
            predicted_outcome = "warning"
            predicted_violation = "speeding"

        # Construct narrative components
        search_text = "A search was conducted" if search_conducted else "No search was conducted"
        drug_text = "was drug-related" if drugs_related_stop else "was not drug-related"
        pronoun = "he" if driver_gender.lower() == "male" else "she"

        # Display markdown summary
        st.markdown(f"""
### 🚔 Prediction Summary

**Predicted Violation:** `{predicted_violation}`  
**Predicted Stop Outcome:** `{predicted_outcome}`  

🚗 A {driver_age}-year-old {driver_gender.lower()} driver was stopped for {predicted_violation.capitalize()} at {stop_time}.  
{search_text}, and {pronoun} received a {predicted_outcome}.  
The stop lasted {stop_duration} and {drug_text}.
        """)
