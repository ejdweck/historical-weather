# Python Version
python>=3.13.0

# API and Web Framework
fastapi>=0.68.0
uvicorn>=0.15.0
python-dotenv>=0.19.0
pydantic-settings>=2.1.0

# Data Processing and Analysis
pandas>=1.3.0
numpy>=1.21.0
scikit-learn>=0.24.2

# Database
psycopg2-binary>=2.9.1
sqlalchemy>=1.4.23

# API Clients
requests>=2.26.0
httpx>=0.18.2

# Testing
pytest>=6.2.5
pytest-asyncio>=0.15.1
pytest-env>=0.8.1

# Development Tools
black>=21.7b0
flake8>=3.9.2

Goal: Build a Python program to predict daily natural gas prices at Transco Zone 6 using historical weather data (via Open-Meteo API) and ICE daily settlement prices. The program will use PostgreSQL for data storage, FastAPI for the backend, and a React-based frontend.

Steps:
Set Up the Project:

Initialize a Python project with the following structure:
    /data          # For raw data or sample files
    /scripts       # For ETL and utility scripts
    /models        # For machine learning models
    /app           # FastAPI backend
    /frontend      # React frontend

Install dependencies & use a virtual environment using venv:

ex:
pip install pandas numpy scikit-learn fastapi uvicorn psycopg2
npx create-react-app frontend

Database Setup:

Use PostgreSQL for data storage with these tables:

Weather Data Table: weather_data
Columns: date, high_temp, low_temp, avg_temp, CDD, HDD.

ICE Settlement Prices Table: settlement_prices
Columns: date, pipeline, settlement_price.

Create a Python script to set up the database schema and connect to the PostgreSQL instance.

Data Collection:

Weather Data:
Use the Open-Meteo API to fetch 5 years of daily weather data.
Include: date, high temp, low temp, avg temp.
ICE Settlement Prices:
Research and manually input or mock sample data for Transco Zone 6's daily settlement prices.

Write a Python script to fetch, clean, and store this data in the PostgreSQL database.

Data Processing:

Add CDD and HDD calculations:
CDD = max(0, avg_temp - 65)
HDD = max(0, 65 - avg_temp)

Update the database to include CDD and HDD values.

Model Development:

Use Scikit-Learn to develop a regression model:

Input features: avg_temp, CDD, HDD.
Target: settlement_price.

Split data into training and testing sets. Evaluate the model using RMSE or R².

Backend Development (FastAPI):

Set up endpoints to:

Fetch historical data for visualization.
Input weather forecast data and predict daily settlement prices.

Example endpoints:

POST /predict: Accepts forecasted weather data and returns predicted price.
GET /data: Retrieves stored weather and settlement price data.

Frontend Development (React):

Create a simple interface with:
Input fields for forecasted weather data.
A submit button to call the POST /predict endpoint.
A section to display prediction results.

Example layout:
Form: Inputs for high_temp, low_temp, avg_temp.
Output: Display predicted price.

Data Update Script:
Create a Python script to:
Fetch the latest weather data from Open-Meteo.
Update the PostgreSQL database.
Use this script to refresh data manually when needed.