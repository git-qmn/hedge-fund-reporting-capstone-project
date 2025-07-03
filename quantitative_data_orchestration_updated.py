import snowflake
# from sympy.physics.vector.printing import params

from db_connection import get_snowflake_connection
from quantitative_data_generation_updated import QuantData
import os
import json
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

def main(as_of_date, params):
    conn = get_snowflake_connection()
    file_name = 'performance_period_mapping.json'
    base_path = os.path.abspath(os.path.dirname(__file__))
    file_path = os.path.join(base_path, file_name)

    with open(file_path) as input_file:
        performance_period_mapping = json.load(input_file)

    quant_obj = QuantData(conn, as_of_date, params, performance_period_mapping)

    if not params.get('generate_accounts'):   
        print('Executing data generation')
        quant_obj.execute_data_generation()

    else:
        print('Generating accounts')
        quant_obj.generate_accounts()


    # # upload csv file to snowflake
    # # Establish the connection
    # conn = get_snowflake_connection()
    # # Create a cursor object
    # cursor = conn.cursor()
    # # Stage the CSV file
    # cursor.execute("PUT file://portfolio_performance_data_test_1.csv @%portfolioperformance")
    # # Copy data into the table
    # cursor.execute("""
    # COPY INTO portfolioperformance
    # FROM @%portfolioperformance
    # FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '|', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    # """)
    # # Close the cursor and connection
    # cursor.close()
    # conn.close()


    # def upload_file_to_snowflake(file_name, table_name):
    #     # Establish the connection
    #     conn = get_snowflake_connection()
    #     # Create a cursor object
    #     cursor = conn.cursor()
    #     try:
    #         # Stage the CSV file with OVERWRITE option
    #         put_command = f"PUT file://{file_name} @%{table_name} OVERWRITE = TRUE"
    #         cursor.execute(put_command)
    #
    #         # Copy data into the table
    #         copy_command = f"""
    #         COPY INTO {table_name}
    #         FROM @%{table_name}
    #         FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '|', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    #         """
    #         cursor.execute(copy_command)
    #         print(f"Data from {file_name} successfully uploaded to {table_name}.")
    #
    #     except snowflake.connector.errors.ProgrammingError as e:
    #         print(f"Programming error: {e}")
    #     except Exception as e:
    #         print(f"An error occurred: {e}")
    #     finally:
    #         # Close the cursor and connection
    #         cursor.close()
    #         conn.close()
    #
    # # Example usage
    # upload_file_to_snowflake("portfolio_performance_data_test.csv", "portfolioperformance")
    #
    # # list file in the stage to avoid the same file name issue
    # # LIST @%{table_name}
    # # We can use OVERWRITE = TRUE to upload the same file name into the stage, but we need make sure all rows are new, otherwise duplicates appear
    # # If no OVERWRITE = TRUE, we must use the different file names everytime until we clean the stage up
    # # clean the stage
    # # REMOVE @%{table_name}

def table_selection(asset_class, pre_cal, select_all_tables, selected_tables):
    table_dict = {}
    other_dict = {}

    all_tables = [ 'portfolio_pre_calculated_performance_data','benchmark_pre_calculated_performance_data','account_benchmark_pre_calculated_performance_data',
                    'holdings_data', 'attribution_data', 'sector_allocation_data', 'region_allocation_data',
                    'transactions_data', 'portfolio_characteristics_data', 'benchmark_characteristics_data',
                    'portfolio_performance_data', 'benchmark_performance_data', 'quality_allocation_data', 'duration_allocation_data',
                    'fixed_income_attribution_data','fixed_income_portfolio_characteristics_data','fixed_income_benchmark_characteristics_data', 'purchases_sales_summary']

    if select_all_tables:
        table_list = ['holdings_data', 'transactions_data','purchases_sales_summary']
        if asset_class == "Equity":
            table_list += ['portfolio_characteristics_data', 'benchmark_characteristics_data', 'attribution_data']
            if pre_cal == True:
                table_list += ['portfolio_pre_calculated_performance_data', 'benchmark_pre_calculated_performance_data', 'account_benchmark_pre_calculated_performance_data', 'sector_allocation_data', 'region_allocation_data']
            else:
                table_list += ['portfolio_performance_data', 'benchmark_performance_data']
        elif asset_class == "Fixed Income":
            table_list += ['fixed_income_portfolio_characteristics_data', 'fixed_income_benchmark_characteristics_data', 'fixed_income_attribution_data', 'quality_allocation_data', 'duration_allocation_data','purchases_sales_summary']
            if pre_cal == True:
                table_list += ['portfolio_pre_calculated_performance_data', 'benchmark_pre_calculated_performance_data', 'account_benchmark_pre_calculated_performance_data', 'sector_allocation_data', 'region_allocation_data']
            else:
                table_list += ['portfolio_performance_data', 'benchmark_performance_data']
        other_list = [item for item in all_tables if item not in table_list]
        table_dict = dict.fromkeys(table_list, True)
        other_dict = {item: False for item in other_list}

    else:
        other_list = [item for item in all_tables if item not in selected_tables]
        table_dict = dict.fromkeys(selected_tables, True)
        other_dict = {item: False for item in other_list}

    return table_dict, other_dict


if __name__ == "__main__":
    base_as_of_date = '2024-11-30'
    as_of_date = '2025-01-31'

    # strategy_code = 'USLCGE'
    # strategy_inception_date = '2009-01-25'
    # strategy_name = 'US Large Cap Growth Equity'
    # asset_class = 'Equity'
    # pre_cal = True

    # strategy_code = 'GVE'
    # strategy_inception_date = '2004-01-25'
    # strategy_name = 'Global Value Equity'
    # asset_class = 'Equity'
    # pre_cal = False

    # strategy_code = 'ENHCRDT'
    # strategy_inception_date = '2012-01-01'
    # strategy_name = 'Fixed Enhanced Credit'
    # asset_class = 'Fixed Income'
    # pre_cal = True

    # strategy_code = 'FICORE'
    # strategy_inception_date = '2010-01-01'
    # strategy_name = 'Fixed Income Core'
    # asset_class = 'Fixed Income'
    # pre_cal = False

    # # as_of_date = '2024-10-31'
    # # as_of_date = '2023-05-31'
    # as_of_date = '2024-11-30'
    # params = {'generate_accounts': False,
    #           'delete_and_insert': True,
    #         # # 'base_original_portfolio_code': '2900',
    #         # # 'base_original_benchmark_code': 'sptotal',
    #         'strategy_code': 'USLCGE',
    #         'strategy_inception_date': '2009-01-25',
    #         'strategy_name': 'US Large Cap Growth Equity',
    #         # # 'base_portfolio_code': 'ASTLCGEMODEL',
    #         # # 'base_benchmark_code': 'sptotal',
    #         # 'strategy_code': 'GVE',
    #         # 'strategy_inception_date': '2004-01-25',
    #         # 'strategy_name': 'Global Value Equity',
    #         # # 'base_portfolio_code': 'ASTGVEMODEL',
    #         # 'strategy_code': 'ENHCRDT',
    #         # 'strategy_inception_date': '2012-01-01',
    #         # 'strategy_name': 'Fixed Enhanced Credit',
    #         # # 'base_portfolio_code': 'ASTFIENHCRMODEL',
    #         # # 'base_benchmark_code': 'H0A0',
    #         # 'strategy_code': 'FICORE',
    #         # 'strategy_inception_date': '2010-01-01',
    #         # 'strategy_name': 'Fixed Income Core',
    #         # # 'base_portfolio_code': 'ASTFICOREMODEL',
    #         # # 'base_benchmark_code': 'LBUSTRUU',
    #         # 'base_as_of_date': '2023-04-30',
    #         # 'base_as_of_date': '2023-05-31',
    #         'base_as_of_date': '2024-10-31',
    #         'portfolio_pre_calculated_performance_data': False,
    #         'benchmark_pre_calculated_performance_data': False,
    #         'account_benchmark_pre_calculated_performance_data': False,
    #         'holdings_data': False,
    #         'attribution_data': False,
    #         'sector_allocation_data': False,
    #         'region_allocation_data': False,
    #         'country_allocation_data': False,
    #         'transactions_data': False,
    #         'portfolio_characteristics_data': False,
    #         'benchmark_characteristics_data': False,
    #         'portfolio_performance_data': False,
    #         'benchmark_performance_data': False,
    #         'quality_allocation_data': False,
    #         'duration_allocation_data': False,
    #         'fixed_income_attribution_data': False,
    #         'fixed_income_portfolio_characteristics_data': False,
    #         'fixed_income_benchmark_characteristics_data': False
    #     }

    delete_and_insert = True
    base_params = {'generate_accounts': False,
              'strategy_code': strategy_code,
              'strategy_inception_date': strategy_inception_date,
              'strategy_name': strategy_name,
              'base_as_of_date': base_as_of_date,
              'delete_and_insert': delete_and_insert
    }

    select_all_tables = True
    # selected_tables = ['holdings_data','attribution_data','purchases_sales_summary']
    # selected_tables = ['attribution_data']
    # selected_tables = ['holdings_data']
    selected_tables = ['purchases_sales_summary']
    table_dict, other_dict =  table_selection(asset_class, pre_cal, select_all_tables, selected_tables)
    params = {**base_params,  **table_dict, **other_dict}
    print(params)
    main(as_of_date, params)

# # Concatenate the new row
# df = pd.concat([df, new_row], ignore_index=True)


# USLCGE: three pre-calculated, sector allocation, region allocation, holdings detail, attribution, transaction details, portfolio characteristics, benchmark characteristics
# GVE: portfolio performance, benchmark performance, holdings details, attribution, portfolio characteristics, benchmark characteristics, transactions
# ENHCRDT: three pre-calculated, holdings detail, sector allocation, region allocation, duration allocation, quality allocation, fixed income attribution, transactions, fixed income portfolio characteristics, fixed income benchmark characteristics
# FICORE: portfolio performance, benchmark performance, holdings detail, duration allocation, quality allocation, fixed income attribution, transactions, fixed income portfolio characteristics, fixed income benchmark characteristics

# use base portfolio code , rep account; get product code and benchmark code
# add a checking function, no duplicates for each portfolio code, time sleep
# condition, automate data insert for strategies with cycle, what's the strategy: equity? fixed income? what are tables I need to insert. configuration list
# first check what's the most recent data for base portfolio code, use that as the asofdate
# delete and insert, each account
