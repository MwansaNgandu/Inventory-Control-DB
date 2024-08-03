import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a DB and create a connection to the SQLite DB
def create_db_connection():
    conn = sqlite3.connect('walmart_sales.db')
    return conn

# Load the data into Pandas DataFrames
sales_data = pd.read.csv('walmart_sales.csv')
macro_data = pd.read.csv('macroeconomic_data.csv')
weather_data = pd.read.csv('wearther_data.csv')
holiday_data = pd.read.csv('holiday_data.csv')

# Display the first few rows of each dataframe
print(sales_data.head())
print(macro_data.head())
print(weather_data.head())
print(holiday_data.head())

# Clean and prepare data: missing values, data type, etc

# DEFINE SQL TABLE SCHEMAS AND INSERT DATA
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales( 
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER,
            product_category TEXT,
            sales REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS macroeconomic(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gdp INTEGER,
            cpi INTEGER,
            c_production REAL,
            unemployment_rate REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER,
            year INTEGER,
            weather_condition TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holiday(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER,
            year INTEGER,
            holiday TEXT
        )
    ''')
    conn.commit()

# Insert the data into tables
def insert_data(conn, data):
    sales_data, macro_data, weather_data, holiday_data = data
    sales_data.to_sql('sales', conn, if_exists='append', index=False)
    macro_data.to_sql('macroeconomic', conn, if_exists='append', index=False)
    weather_data.to_sql('weather', conn, if_exists='append', index=False)
    holiday_data.to_sql('holidays', conn, if_exists='append', index=False)
    conn.commit()

def load_data(conn):
    # Load sales data
    sales_data = pd.read_sql_query('SELECT * FROM sales', conn)
    # Load macroeconomic data
    macro_data = pd.read_sql_query('SELECT * FROM macroeconomic', conn)
    # Load weather data
    weather_data = pd.read_sql_query('SELECT * FROM weather', conn)
    # Load holiday data
    holiday_data = pd.read_sql_query('SELECT * FROM holidays', conn)
    return sales_data, macro_data, weather_data, holiday_data

def clean_data(data):
    sales_data, macro_data, weather_data, holiday_data = data
    # Clean sales data
    sales_data.dropna(inplace=True)
    # Clean macroeconomic data
    macro_data.dropna(inplace=True)
    # Clean weather data
    weather_data.dropna(inplace=True)
    # Clean holiday data
    holiday_data.dropna(inplace=True)
    return sales_data, macro_data, weather_data, holiday_data

# PERFORM SQL QUERIES
def perform_queries(conn):
    # Which year had the highest sales?
    highest_sales_year = pd.read_sql_query('''
        SELECT year, SUM(sales) as total_sales
        FROM sales
        GROUP BY year
        ORDER BY total_sales DESC
        LIMIT 1
    ''', conn)
    print(highest_sales_year)

    # How was the weather during the year of highest sales?
    year = highest_sales_year['year'].iloc[0]
    weather_during_highest_sales = pd.read_sql_query(f'''
        SELECT year, month, weather_condition
        FROM weather
        WHERE year = {year}
    ''', conn)
    print(weather_during_highest_sales)

    # Impact of weather on sales
    weather_sales = pd.read_sql_query('''
        SELECT w.weather_condition, SUM(s.sales) as total_sales
        FROM sales s
        JOIN weather w ON s.year = w.year AND s.month = w.month
        GROUP BY w.weather_condition
        ORDER BY total_sales DESC
    ''', conn)
    print(weather_sales)

    # Sales near holiday seasons
    sales_during_holidays = pd.read_sql_query('''
        SELECT s.year, s.month, SUM(s.sales) as total_sales
        FROM sales s
        JOIN holidays h ON s.year = h.year AND s.month = h.month
        GROUP BY s.year, s.month
        ORDER BY total_sales DESC
    ''', conn)
    print(sales_during_holidays)

    # Relationships between sales and macroeconomic variables
    sales_macro = pd.read_sql_query('''
        SELECT s.year, SUM(s.sales) as total_sales, m.gdp, m.cpi, m.c_production, m.unemployment_rate
        FROM sales s
        JOIN macroeconomic m ON s.year = m.year
        GROUP BY s.year, m.gdp, m.cpi, m.c_production, m.unemployment_rate
    ''', conn)
    print(sales_macro)

    return highest_sales_year, weather_sales, sales_during_holidays, sales_macro

# VISUALIZE RESULTS
def visualize_results(data, highest_sales_year, weather_sales, sales_during_holidays, sales_macro):
    sales_data, macro_data, weather_data, holiday_data = data

    # Plot sales over the years
    plt.figure(figsize=(10,6))
    sns.barplot(x='year', y='total_sales', data=highest_sales_year)
    plt.title('Total Sales per Year')
    plt.show()

    # Plot sales vs. weather conditions
    plt.figure(figsize=(10, 6))
    sns.barplot(x='weather_condition', y='total_sales', data=weather_sales)
    plt.title('Sales vs. Weather Conditions')
    plt.show()

    # Plot sales during holiday seasons
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='month', y='total_sales', hue='year', data=sales_during_holidays)
    plt.title('Sales During Holiday Seasons')
    plt.show()

    # Plot sales vs. macroeconomic variables
    sns.pairplot(sales_macro, x_vars=['gdp', 'cpi', 'c_production', 'unemployment_rate'], y_vars='total_sales', height=5, aspect=0.8)
    plt.show()

# Main function
def main():
    conn = create_db_connection()
    data = load_data(conn)
    clean_data(data)
    create_tables(conn)
    insert_data(conn, data)
    highest_sales_year, weather_sales, sales_during_holidays, sales_macro = perform_queries(conn)
    visualize_results(data, highest_sales_year, weather_sales, sales_during_holidays, sales_macro)
    conn.close()

if __name__ == '__main__':
    main()