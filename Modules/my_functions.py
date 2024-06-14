import pandas as pd
from decimal import Decimal
from pyspark.sql.types import StringType, IntegerType, DateType
from datetime import datetime
from pyspark.sql.functions import col, when, sum as _sum

# Function to remove leading zeros from a client number
def remove_leading_zeros(client_number):
    return str(int(client_number))

# Function to convert 'yes'/'no' to 'Y'/'N'
def convert_to_yn(value):
    if pd.isnull(value):
        return "NA"
    return "Y" if value.lower() in ["yes", "y"] else "N"

# Function to convert values to integers
def to_int(val):
    try:
        return int(val)
    except:
        return None

# Function to convert values to decimals with two decimal places
def to_decimal(val):
    try:
        return Decimal(val).quantize(Decimal('0.00'))
    except:
        return None

# Function to convert strings to dates
def to_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except:
        return None
    
# Function to calculate the amount in EUR
def calculate_amount_eur(original_amount, original_currency, exchange_rate):
    if original_currency == 'EUR':
        return original_amount
    else:
        return original_amount * exchange_rate
    
# Function to convert 'Y'/'N' to boolean values
def yn_to_bool(val):
    if pd.isnull(val):
        return None
    return True if val == 'Y' else False

# Function to determine the size of the enterprise
def determine_enterprise_size(num_employees):
    if num_employees < 100:
        return 'S'
    elif 100 <= num_employees < 1000:
        return 'M'
    else:
        return 'L'
    
# Function to calculate SecuredAmountEUR
def calculate_secured_amount(df):
    df['SecuredAmountEUR'] = 0.0
    group_max_secured_amount = 500000

    for group, group_df in df.groupby('ClientGroup'):
        remaining_secured_amount = group_max_secured_amount
        for index, row in group_df.iterrows():
            if pd.isna(row['ClientSecuredIND']):
                secured_ind = False
            else:
                secured_ind = row['ClientSecuredIND']

            if secured_ind and remaining_secured_amount > 0:
                if row['AmountEUR'] <= remaining_secured_amount:
                    df.at[index, 'SecuredAmountEUR'] = row['AmountEUR']
                    remaining_secured_amount -= row['AmountEUR']
                else:
                    df.at[index, 'SecuredAmountEUR'] = remaining_secured_amount
                    remaining_secured_amount = 0
            else:
                df.at[index, 'SecuredAmountEUR'] = 0
                df.at[index, 'ClientSecuredIND'] = False
    return df
