import pytest
import pandas as pd
from decimal import Decimal
from datetime import datetime
import my_functions

def test_remove_leading_zeros():
    assert my_functions.remove_leading_zeros('001234') == '1234'
    assert my_functions.remove_leading_zeros('000567') == '567'
    assert my_functions.remove_leading_zeros('000000') == '0'
    assert my_functions.remove_leading_zeros('1000') == '1000'

def test_convert_to_yn():
    assert my_functions.convert_to_yn('yes') == 'Y'
    assert my_functions.convert_to_yn('no') == 'N'
    assert my_functions.convert_to_yn('y') == 'Y'
    assert my_functions.convert_to_yn('n') == 'N'
    assert my_functions.convert_to_yn(None) == 'NA'
    assert my_functions.convert_to_yn('') == 'N'
    assert my_functions.convert_to_yn('maybe') == 'N'

def test_to_int():
    assert my_functions.to_int('123') == 123
    assert my_functions.to_int('0') == 0
    assert my_functions.to_int('001') == 1
    assert my_functions.to_int('abc') is None
    assert my_functions.to_int('12.34') is None
    assert my_functions.to_int('') is None
    assert my_functions.to_int(None) is None

def test_to_decimal():
    assert my_functions.to_decimal('123.456') == Decimal('123.46')
    assert my_functions.to_decimal('123') == Decimal('123.00')
    assert my_functions.to_decimal('abc') is None
    assert my_functions.to_decimal('') is None

def test_to_date():
    assert my_functions.to_date('2023-01-01') == datetime(2023, 1, 1)
    assert my_functions.to_date('invalid-date') is None

def test_calculate_amount_eur():
    assert my_functions.calculate_amount_eur(100, 'EUR', 1.1) == 100
    assert my_functions.calculate_amount_eur(100, 'USD', 0.9) == 90

def test_yn_to_bool():
    assert my_functions.yn_to_bool('Y') is True
    assert my_functions.yn_to_bool('N') is False
    assert my_functions.yn_to_bool(None) is None
    assert my_functions.yn_to_bool('') is False

def test_determine_enterprise_size():
    assert my_functions.determine_enterprise_size(50) == 'S'
    assert my_functions.determine_enterprise_size(500) == 'M'
    assert my_functions.determine_enterprise_size(1500) == 'L'

def test_calculate_secured_amount():
    data = {
        'ClientGroup': ['A', 'A', 'B'],
        'ClientSecuredIND': [True, False, True],
        'AmountEUR': [300000, 200000, 600000]}
    df = pd.DataFrame(data)
    result_df = my_functions.calculate_secured_amount(df)
    assert result_df.at[0, 'SecuredAmountEUR'] == 300000
    assert result_df.at[1, 'SecuredAmountEUR'] == 0
    assert result_df.at[2, 'SecuredAmountEUR'] == 500000
