import pandas as pd
import numpy as np
import decimal
from decimal import Decimal
from DeriveValidPeriods import exec_script as ext
from DeriveValidPeriods import read_data as rdt
import string
import random
import snowflake
from db_connection import get_snowflake_connection
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta, date
from open_ai_interactions import get_openai_client_obj, interact_with_chat_application, interact_with_gpt4

def add_random_float(x, min = -1, max = 1):
    if np.issubdtype(type(x), np.number):
        random_num = np.random.uniform(min, max)
        return x + random_num
    if isinstance(x, decimal.Decimal):
        random_num = Decimal(np.random.uniform(min, max))
        return x + random_num
    return x


def generate_random_dates(start_date, end_date, num_dates):
    """
    Generates a list of random dates within a specified range.

    Parameters:
    - start_date (str): The start date in 'YYYY-MM-DD' format.
    - end_date (str): The end date in 'YYYY-MM-DD' format.
    - num_dates (int): The number of random dates to generate.

    Returns:
    - List of random dates as strings in 'YYYY-MM-DD' format.
    """
    # Convert start and end dates to pandas timestamps
    start_ts = pd.to_datetime(start_date).value // 10**9  # Convert to Unix timestamp
    end_ts = pd.to_datetime(end_date).value // 10**9

    # Generate random integers between start and end timestamps
    random_timestamps = np.random.randint(start_ts, end_ts, num_dates)

    # Convert integers back to dates
    random_dates = [pd.to_datetime(ts, unit='s').strftime('%Y-%m-%d') for ts in random_timestamps]

    return random_dates


def add_random_to_columns(df, column_ranges, non_negative_fields,seed=None):
    """
    Adds random float values within specified ranges to columns in a DataFrame.

    Parameters:
    - df: pandas.DataFrame to modify.
    - column_ranges: Dict where keys are column names and values are tuples specifying the (min, max) range of random numbers to add.
    - seed: Optional seed for reproducibility.

    Returns:
    - Modified DataFrame with random numbers added to specified columns.
    """
    if seed is not None:
        np.random.seed(seed)
    
    # def add_random_float_to_column(x, is_non_negative, min_val, max_val):
    #     random_addition = np.random.uniform(min_val, max_val)
    #     result = x + random_addition
    #     if is_non_negative and result < 0:
    #         return x + np.random.uniform(0, max_val)
    #     else:
    #         return result
    def add_random_float_to_column(x, is_non_negative, min_val, max_val):
        if np.issubdtype(type(x), np.number):
            random_addition = np.random.uniform(min_val, max_val)
            result = x + random_addition
            if is_non_negative and result < 0:
                return x + np.random.uniform(0, max_val)
            else:
                return result
        if isinstance(x, decimal.Decimal):
            random_addition = Decimal(np.random.uniform(min_val, max_val))
            result = x + random_addition
            if is_non_negative and result < 0:
                return x + Decimal(np.random.uniform(0, max_val))
            else:
                return result
        return x
    
    for column, (min_val, max_val) in column_ranges.items():
        if column in df.columns:
            # Apply a random number within the specified range to each element in the column
            if column in non_negative_fields:
                df[column] = df[column].apply(lambda x: add_random_float_to_column(x, is_non_negative=True, min_val=min_val, max_val=max_val))
            else:
                df[column] = df[column].apply(lambda x: add_random_float_to_column(x, is_non_negative=False, min_val=min_val, max_val=max_val))
    
    return df

def generate_account_codes(num_accounts):
    return [''.join(random.choices(string.ascii_uppercase + string.digits, k=8)) for _ in range(num_accounts)]

def adjust_sum_of_column(df, column_name, sum_value):
    """
    Validates that the sum of a column in a DataFrame matches a specified value.

    Parameters:
    - df: pandas.DataFrame to validate.
    - column_name: Name of the column to sum.
    - sum_value: sum value.

    Returns:
    - a dataframe with the column values adjusted to match the specified sum.
    """
    current_sum = df[column_name].sum()
    misaligned_sum = current_sum - sum_value
    if misaligned_sum != 0:
        non_zero_rows = df[df[column_name] != 0].shape[0]
        # print(df)
        # print("non_zero_rows:", non_zero_rows)
        if non_zero_rows > 0:
            reassign_value = misaligned_sum / non_zero_rows

            for row_index, row in df.iterrows():
                if row[column_name] != 0:
                    df.at[row_index, column_name] = row[column_name] - reassign_value
    
    return df

def adjust_sum_of_columns_no_negative(df, column_name, sum_value):
    current_sum = df[column_name].sum()
    misaligned_sum = current_sum - sum_value
    if misaligned_sum != 0:
        non_zero_rows = df[df[column_name] != 0].shape[0]
        if non_zero_rows > 0:
            reassign_value = misaligned_sum / non_zero_rows

            for row_index, row in df.iterrows():
                if pd.notnull(row[column_name]) and row[column_name] != 0:
                    df.at[row_index, column_name] = float(row[column_name]) - float(reassign_value)

    # Ensure all values are greater than or equal to 0
    df[column_name] = df[column_name].apply(lambda x: max(float(x), 0) if pd.notnull(x) else x)

    # Adjust the sum again to match the target value, if current_sum is not zero
    current_sum = df[column_name].sum()
    if current_sum != 0:
        adjustment_factor = sum_value / current_sum
        df[column_name] = df[column_name].apply(lambda x: x * adjustment_factor if pd.notnull(x) else x)

    return df


class QuantData:
    def __init__(self, conn, as_of_date, params, performance_period_mapping):
        self.conn = conn
        self.as_of_date = as_of_date
        self.params = params
        self.delete_and_insert = self.params['delete_and_insert']
        self.strategy_code = self.params['strategy_code']
        self.base_portfolio_code = conn.cursor().execute(
                    f"SELECT REPRESENTATIVEACCOUNT FROM PRODUCTMASTER WHERE STRATEGY = '{self.strategy_code}'").fetchone()[0]
        self.product_code = conn.cursor().execute(
                    f"SELECT PRODUCTCODE FROM PRODUCTMASTER WHERE STRATEGY = '{self.strategy_code}'").fetchone()[0]
        self.base_as_of_date = self.params['base_as_of_date']
        self.base_benchmark_code = conn.cursor().execute(
                    f"SELECT BENCHMARKCODE FROM PORTFOLIOBENCHMARKASSOCIATION WHERE PORTFOLIOCODE = '{self.base_portfolio_code}'").fetchone()[0]
        self.performance_period_mapping = performance_period_mapping
        self.portfolio_general_info = self.conn.cursor().execute(f"select pgi.portfoliocode, pgi.name, pgi.investmentstyle, pgi.portfoliocategory, pgi.opendate, pgi.performanceinceptiondate, pgi.terminationdate, pgi.basecurrencycode, pgi.basecurrencyname, pgi.isbeginofdayperformance, pgi.productcode "
                                                                 f"from portfoliogeneralinformation pgi "
                                                                 f"inner join portfolioattributes pt on pt.portfoliocode = pgi.portfoliocode "
                                                                 f"where pt.attributetypecode = '{self.strategy_code}'")
        self.portfolio_general_info_df = pd.DataFrame.from_records(iter(self.portfolio_general_info), columns=[x[0] for x in self.portfolio_general_info.description])

        self.openai_client = get_openai_client_obj()

    def generate_accounts(self):
        account_code_list = generate_account_codes(50)
        account_df = pd.DataFrame(columns=['PORTFOLIOCODE','NAME', 'INVESTMENTSTYLE', 'PORTFOLIOCATEGORY','OPENDATE','PERFORMANCEINCEPTIONDATE','TERMINATIONDATE','BASECURRENCYCODE','BASECURRENCYNAME','ISBEGINOFDAYPERFORMANCE','PRODUCTCODE'])
        # account_name_request = f"You are generating synthetic data for an asset management firm. Generate 50 comma separated names appropriate for separately managed accounts managed under the strategy name {self.params['strategy_name']} and do not include numbering"
        account_name_request = f"You are generating synthetic data for an asset management firm. Generate 50 comma separated names appropriate for separately managed accounts managed under the strategy name {self.params['strategy_name']} and do not include numbering"
        message_text = [{"role": "system", "content": "you are a synthetic data generator for an asset management firm. do not include any text other than the required answer and act as a completion service"},{"role": "user", "content": account_name_request}]
        content = interact_with_gpt4(message_text, self.openai_client)['choices'][0]['message']['content']
        account_names_list = content.split(',')
        strategy_inception_date = self.params['strategy_inception_date']
        inception_date_list = generate_random_dates(strategy_inception_date, '2023-04-30', 50)

        for account_code, account_name, inception_date in zip(account_code_list, account_names_list, inception_date_list):
            # account_df = account_df.append({'PORTFOLIOCODE': account_code, 'NAME': account_name, 'INVESTMENTSTYLE': 'Growth', 'PORTFOLIOCATEGORY': 'Individual Account', 'OPENDATE': inception_date, 'PERFORMANCEINCEPTIONDATE': inception_date, 'TERMINATIONDATE': None, 'BASECURRENCYCODE': 'USD', 'BASECURRENCYNAME': 'US Dollar', 'ISBEGINOFDAYPERFORMANCE': False, 'PRODUCTOCDE': 'ENHCRDTSMA'}, ignore_index=True)
            new_data = {'PORTFOLIOCODE': account_code, 'NAME': account_name, 'INVESTMENTSTYLE': 'Growth', 'PORTFOLIOCATEGORY': 'Individual Account', 'OPENDATE': inception_date, 'PERFORMANCEINCEPTIONDATE': inception_date, 'TERMINATIONDATE': None, 'BASECURRENCYCODE': 'USD', 'BASECURRENCYNAME': 'US Dollar', 'ISBEGINOFDAYPERFORMANCE': False, 'PRODUCTCODE': f'{self.product_code}'}
            # new_data = {'PORTFOLIOCODE': account_code, 'NAME': account_name, 'INVESTMENTSTYLE': 'Growth',
            #             'PORTFOLIOCATEGORY': 'Individual Account', 'OPENDATE': inception_date,
            #             'PERFORMANCEINCEPTIONDATE': inception_date, 'TERMINATIONDATE': None, 'BASECURRENCYCODE': 'USD',
            #             'BASECURRENCYNAME': 'US Dollar', 'ISBEGINOFDAYPERFORMANCE': False, 'PRODUCTOCDE': None}
            new_data_df = pd.DataFrame([new_data])
            account_df = pd.concat([account_df, new_data_df], ignore_index=True)
        account_df.to_csv('account_data.csv', index=False, sep='|')

        # for account_code in account_code_list:
            
    def insert_data(self, df, table_name):
        # table_name is case sensitive, must match what the name is in the snowflake
        # Establish the connection
        conn = get_snowflake_connection()
        # try:
        # Upload the DataFrame to Snowflake
        # success, nchunks, nrows, _ = write_pandas(conn, df, table_name)

        df.index = pd.RangeIndex(start=0, stop=len(df), step=1)
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name, auto_create_table=False)
        if success:
            print(f"DataFrame successfully uploaded to {table_name}. Rows inserted: {nrows}")
        else:
            print("Failed to upload DataFrame to Snowflake.")
        # except snowflake.connector.errors.ProgrammingError as e:
        #     print(f"Programming error: {e}")
        # except Exception as e:
        #     print(f"An error occurred: {e}")
        # finally:
            # Close the connection
        conn.close()

        # # Example usage
        # df = pd.read_csv("portfolio_performance_data_test.csv")  # Example DataFrame creation
        # self.upload_dataframe_to_snowflake(df, "PORTFOLIOPERFORMANCE")

    def execute_data_generation(self):
        # Those below are for USLCGE
        if self.params['portfolio_pre_calculated_performance_data']:
            pre_calc_port_return_df = self.create_portfolio_pre_calculated_performance_data()
            if not pre_calc_port_return_df.empty:
                # Convert all date columns to date
                date_columns = ['HISTORYDATE', 'PERFORMANCEINCEPTIONDATE', 'PORTFOLIOINCEPTIONDATE']
                for col in date_columns:
                    pre_calc_port_return_df[col] = pd.to_datetime(pre_calc_port_return_df[col],errors='coerce').dt.date
                self.insert_data(pre_calc_port_return_df, "PRECALCULATEDPORTFOLIOPERFORMANCE")
        if self.params['benchmark_pre_calculated_performance_data']:
            pre_calc_bench_return_df = self.create_benchmark_pre_calculated_performance_data()
            if not pre_calc_bench_return_df.empty:
                pre_calc_bench_return_df['HISTORYDATE'] = pd.to_datetime(pre_calc_bench_return_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(pre_calc_bench_return_df, "PRECALCULATEDBENCHMARKPERFORMANCE")
        if self.params['account_benchmark_pre_calculated_performance_data']:
            pre_calc_acc_return_df = self.create_account_benchmark_pre_calculated_performance_data()
            if not pre_calc_acc_return_df.empty:
                pre_calc_acc_return_df['HISTORYDATE'] = pd.to_datetime(pre_calc_acc_return_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(pre_calc_acc_return_df, "PRECALCULATEDACCOUNTBENCHMARKPERFORMANCE")
        # Those below are for GVE
        if self.params['holdings_data']:
            holdings_df = self.create_holdings_data()
            if not holdings_df.empty:
                holdings_df['HISTORYDATE'] = pd.to_datetime(holdings_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(holdings_df, "HOLDINGSDETAILS")
        
        if self.params['attribution_data']:
            attribution_df = self.create_attribution_data()
            if not attribution_df.empty:
                attribution_df['HISTORYDATE'] = pd.to_datetime(attribution_df['HISTORYDATE'], errors='coerce').dt.date
                self.insert_data(attribution_df, "ATTRIBUTION")

        if self.params['purchases_sales_summary']:
            purchases_sales_df = self.create_purchases_sales_summary()
            if not purchases_sales_df.empty:
                purchases_sales_df['HISTORYDATE'] = pd.to_datetime(purchases_sales_df['HISTORYDATE'], errors='coerce').dt.date
                self.insert_data(purchases_sales_df, "PURCHASESANDSALESSUMMARY")

        if self.params['sector_allocation_data']:
            sector_allocation_df = self.create_sector_allocation_data()
            if not sector_allocation_df.empty:
                sector_allocation_df['HISTORYDATE'] = pd.to_datetime(sector_allocation_df['HISTORYDATE'], errors='coerce').dt.date
                self.insert_data(sector_allocation_df, "SECTORALLOCATION")

        if self.params['region_allocation_data']:
            region_allocation_df = self.create_region_allocation_data()
            if not region_allocation_df.empty:
                region_allocation_df['HISTORYDATE'] = pd.to_datetime(region_allocation_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(region_allocation_df, "REGIONALLOCATION")
        if self.params['transactions_data']:
            transactions_df = self.create_transactions_data()
            if not transactions_df.empty:
                date_columns = ['TRADEDATE', 'SETTLEDATE']
                for col in date_columns:
                    transactions_df[col] = pd.to_datetime(transactions_df[col],errors='coerce').dt.date
                self.insert_data(transactions_df, "TRANSACTIONDETAILS")
        if self.params['portfolio_characteristics_data']:
            port_characteristics_df = self.create_portfolio_characteristics_data()
            if not port_characteristics_df.empty:
                port_characteristics_df['HISTORYDATE'] = pd.to_datetime(port_characteristics_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(port_characteristics_df, "PORTFOLIOCHARACTERISTICS")
        if self.params['benchmark_characteristics_data']:
            bench_characteristics_df = self.create_benchmark_characteristics_data()
            if not bench_characteristics_df.empty:
                bench_characteristics_df['HISTORYDATE'] = pd.to_datetime(bench_characteristics_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(bench_characteristics_df, "BENCHMARKCHARACTERISTICS")
        if self.params['portfolio_performance_data']:
            # port_performance_batches = self.create_performance_factors()
            # for i, port_performance_df in enumerate(port_performance_batches):
            #     date_columns = ['HISTORYDATE', 'PERFORMANCEINCEPTIONDATE', 'PORTFOLIOINCEPTIONDATE']
            #     # Seems like this table those dates are timestamp format
            #     for col in date_columns:
            #         port_performance_df[col] = pd.to_datetime(port_performance_df[col], errors='coerce')
            #         port_performance_df[col] = port_performance_df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            #     self.insert_data(port_performance_df, "PORTFOLIOPERFORMANCE")
            port_performance_df = self.create_performance_factors()
            if not port_performance_df.empty:
                date_columns = ['HISTORYDATE', 'PERFORMANCEINCEPTIONDATE', 'PORTFOLIOINCEPTIONDATE']
                # Seems like this table those dates are timestamp format
                for col in date_columns:
                    port_performance_df[col] = pd.to_datetime(port_performance_df[col], errors='coerce')
                    port_performance_df[col] = port_performance_df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                self.insert_data(port_performance_df, "PORTFOLIOPERFORMANCE")
        if self.params['benchmark_performance_data']:
            bench_performance_df = self.create_benchmark_prices()
            if not bench_performance_df.empty:
                bench_performance_df['HISTORYDATE'] = pd.to_datetime(bench_performance_df['HISTORYDATE'],errors='coerce')
                self.insert_data(bench_performance_df, "BENCHMARKPERFORMANCE")
        if self.params['quality_allocation_data']:
            quality_df = self.create_credit_quality_allocation_data()
            if not quality_df.empty:
                quality_df['HISTORYDATE'] = pd.to_datetime(quality_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(quality_df, "CREDITQUALITYALLOCATION")
        if self.params['duration_allocation_data']:
            duration_df = self.create_duration_allocation_data()
            if not duration_df.empty:
                duration_df['HISTORYDATE'] = pd.to_datetime(duration_df['HISTORYDATE'], errors='coerce').dt.date
                self.insert_data(duration_df, "DURATIONALLOCATION")
        if self.params['fixed_income_attribution_data']:
            fi_attribution_df = self.create_fixed_income_attribution_data()
            if not fi_attribution_df.empty:
                fi_attribution_df['HISTORYDATE'] = pd.to_datetime(fi_attribution_df['HISTORYDATE'], errors='coerce').dt.date
                self.insert_data(fi_attribution_df, "FIXEDINCOMEATTRIBUTION")
        if self.params['fixed_income_portfolio_characteristics_data']:
            fi_pr_characteristics_df = self.create_fixed_income_portfolio_characteristics_data()
            if not fi_pr_characteristics_df.empty:
                fi_pr_characteristics_df['HISTORYDATE'] = pd.to_datetime(fi_pr_characteristics_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(fi_pr_characteristics_df, "PORTFOLIOCHARACTERISTICS")
        if self.params['fixed_income_benchmark_characteristics_data']:
            fi_bench_characteristics_df = self.create_fixed_income_benchmark_characteristics_data()
            if not fi_bench_characteristics_df.empty:
                fi_bench_characteristics_df['HISTORYDATE'] = pd.to_datetime(fi_bench_characteristics_df['HISTORYDATE'],errors='coerce').dt.date
                self.insert_data(fi_bench_characteristics_df, "BENCHMARKCHARACTERISTICS")

        self.conn.close()

    def create_portfolio_pre_calculated_performance_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM precalculatedportfolioperformance WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and PerformanceCategory = 'Asset Class' and PerformanceCategoryname = 'Total Portfolio'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedportfolioperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and PerformanceCategory = 'Asset Class' and PerformanceCategoryname = 'Total Portfolio'")
        port_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        required_period_list = list(self.performance_period_mapping.keys())
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        # as_of_date = self.as_of_date
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        # print(as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM precalculatedportfolioperformance WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                valid_period_params = {'AsofDate': as_of_date, 'InceptionDate': str(row['PERFORMANCEINCEPTIONDATE']),
                                       'FiscalYearEnd': '06-30', 'SuppressNotApplicablePeriods': 'yes',
                                       'SuppressDuplicatePeriods': 'no', 'PeriodList': ','.join(required_period_list)}
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = port_performance_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                temp_df['PERFORMANCEINCEPTIONDATE'] = row['PERFORMANCEINCEPTIONDATE']
                temp_df['PORTFOLIOINCEPTIONDATE'] = row['OPENDATE']
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM precalculatedportfolioperformance WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = temp_df.map(add_random_float)
                        valid_period_response = ext(rdt, valid_period_params)
                        valid_periods_df = pd.DataFrame.from_records(valid_period_response['data'])
                        period_list = valid_periods_df['period'].unique().tolist()
                        for period_key in self.performance_period_mapping.keys():
                            if period_key not in period_list:
                                temp_df[self.performance_period_mapping[period_key]] = np.nan
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = temp_df.map(add_random_float)
                    valid_period_response = ext(rdt, valid_period_params)
                    valid_periods_df = pd.DataFrame.from_records(valid_period_response['data'])
                    period_list = valid_periods_df['period'].unique().tolist()
                    for period_key in self.performance_period_mapping.keys():
                        if period_key not in period_list:
                            temp_df[self.performance_period_mapping[period_key]] = np.nan
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                port_performance_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date) & (result_df[
                                                                            'PERFORMANCECATEGORY'] == 'Asset Class') & (
                                                                        result_df[
                                                                            'PERFORMANCECATEGORYNAME'] == 'Total Portfolio')]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from precalculatedportfolioperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and PerformanceCategory = 'Asset Class' and PerformanceCategoryname = 'Total Portfolio'")
                port_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                            columns=[x[0] for x in
                                                                                     sample_data_info.description])
        return result_df

    def create_benchmark_pre_calculated_performance_data(self):
        general_info = self.conn.cursor().execute(f"SELECT DISTINCT(b.BENCHMARKCODE) FROM PORTFOLIOBENCHMARKASSOCIATION b "
                                                  f"INNER JOIN PORTFOLIOATTRIBUTES p ON p.PORTFOLIOCODE = b.PORTFOLIOCODE "
                                                  f"WHERE p.ATTRIBUTETYPECODE = '{self.strategy_code}'")
        general_info_df = pd.DataFrame.from_records(iter(general_info), columns=[x[0] for x in general_info.description])
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM precalculatedbenchmarkperformance WHERE BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        # print("The actual base as of date is: ", base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedbenchmarkperformance where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
        benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT BENCHMARKCODE, COUNT(*) FROM precalculatedbenchmarkperformance WHERE HISTORYDATE = '{as_of_date}' GROUP BY BENCHMARKCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                benchmark_code = row['BENCHMARKCODE']
                temp_df = benchmark_performance_sample_data_df.copy(deep=True)
                temp_df['BENCHMARKCODE'] = benchmark_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == benchmark_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM precalculatedbenchmarkperformance WHERE BENCHMARKCODE = '{benchmark_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = temp_df.map(add_random_float)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = temp_df.map(add_random_float)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                benchmark_performance_sample_data_df = result_df[(result_df['BENCHMARKCODE'] == self.base_benchmark_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from precalculatedbenchmarkperformance where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
                benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                 columns=[x[0] for x in
                                                                                          sample_data_info.description])
        return result_df
    # if not result_df.empty:

    
    def create_account_benchmark_pre_calculated_performance_data(self):
        general_info = self.conn.cursor().execute(f"SELECT DISTINCT b.PORTFOLIOCODE, b.BENCHMARKCODE FROM PORTFOLIOBENCHMARKASSOCIATION b "
                                                  f"INNER JOIN PORTFOLIOATTRIBUTES p ON p.PORTFOLIOCODE = b.PORTFOLIOCODE "
                                                  f"WHERE p.ATTRIBUTETYPECODE = '{self.strategy_code}'")
        general_info_df = pd.DataFrame.from_records(iter(general_info), columns=[x[0] for x in general_info.description])
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM precalculatedaccountbenchmarkperformance WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedaccountbenchmarkperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and BENCHMARKCODE = '{self.base_benchmark_code}'")
        account_benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])

        # # Values we want to add
        # values = [1.254367, 4.563289, -0.753248, 3.245789, 2.567893, 5.678932, 7.324589, 6.987654, 6.578934, 7.123456,
        #           5.987321, 7.654321, 6.789012, 6.345678, 7.890123, 6.901234]
        #
        # # Column names in order
        # columns = ["QUARTERTODATE", "FISCALYEARTODATE", "PRIORFISCALQUARTER1", "PRIORFISCALQUARTER2",
        #            "PRIORFISCALQUARTER3", "PRIORFISCALQUARTER4", "PRIORFISCALYEAR1", "PRIORFISCALYEAR2",
        #            "PRIORFISCALYEAR3", "PRIORFISCALYEAR4", "PRIORFISCALYEAR5", "PRIORFISCALYEAR6", "PRIORFISCALYEAR7",
        #            "PRIORFISCALYEAR8", "PRIORFISCALYEAR9", "PRIORFISCALYEAR10"]
        #
        # # Synthetic some data for empty time periods
        # account_benchmark_performance_sample_data_df.loc[0, columns] = values
        # # # Display the DataFrame
        # # print(account_benchmark_performance_sample_data_df)

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, BENCHMARKCODE, COUNT(*) FROM precalculatedaccountbenchmarkperformance WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE, BENCHMARKCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                benchmark_code = row['BENCHMARKCODE']
                temp_df = account_benchmark_performance_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['BENCHMARKCODE'] = benchmark_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code and item[1] == benchmark_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM precalculatedaccountbenchmarkperformance WHERE PORTFOLIOCODE = '{portfolio_code}' and BENCHMARKCODE = '{benchmark_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = temp_df.map(add_random_float)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = temp_df.map(add_random_float)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                account_benchmark_performance_sample_data_df = result_df[(result_df['BENCHMARKCODE'] == self.base_benchmark_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date) & (result_df['PORTFOLIOCODE'] == self.base_portfolio_code)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from precalculatedaccountbenchmarkperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and BENCHMARKCODE = '{self.base_benchmark_code}'")
                account_benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                         columns=[x[0] for x in
                                                                                                  sample_data_info.description])
        return result_df

    def create_holdings_data(self):
        print("Creating holdings data")
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM HOLDINGSDETAILS WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select * from HOLDINGSDETAILS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"QUANTITY":(-100,100), "MARKETVALUEWITHOUTACCRUEDINCOME":(-10000,10000), "LOCALMARKETVALUE": (-10000,10000), "UNREALIZEDGAINSLOSSES":(-1000,1000), "ACCRUEDINCOME":(-100,100), "ESTIMATEDANNUALINCOME":(-250,250), "PRICE": (-10,10) }
        non_negative_fields = ["QUANTITY", "MARKETVALUEWITHOUTACCRUEDINCOME", "LOCALMARKETVALUE", "ESTIMATEDANNUALINCOME", "PRICE"]
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM HOLDINGSDETAILS WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                print("creating holdings data for ", as_of_date, " for portfolio ", row['PORTFOLIOCODE'])
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = holdings_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM HOLDINGSDETAILS WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        temp_df['MARKETVALUE'] = temp_df['MARKETVALUEWITHOUTACCRUEDINCOME'] + temp_df['ACCRUEDINCOME']
                        temp_df['PORTFOLIOWEIGHT'] = 100 * temp_df['MARKETVALUE'] / temp_df['MARKETVALUE'].sum()
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                holdings_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                         result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from HOLDINGSDETAILS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in
                                                                                                     sample_data_info.description])
        return result_df
        

    def create_sector_allocation_data(self):
        #NOTE : The sector allocation data will depend on the holdings' data. Make sure to generate the synthetic holdings data before generating the sector allocation data
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM SECTORALLOCATION WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM SECTORALLOCATION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                sample_data_info = self.conn.cursor().execute(
                    f"select PORTFOLIOCODE, PRIMARYSECTORSCHEME, PRIMARYSECTORNAME, MARKETVALUEWITHOUTACCRUEDINCOME, MARKETVALUE, HISTORYDATE from  HOLDINGSDETAILS where PORTFOLIOCODE='{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                    columns=[x[0] for x in
                                                                             sample_data_info.description])
                # print(holdings_sample_data_df)
                temp_df = holdings_sample_data_df.copy(deep=True)
                temp_df = temp_df.groupby('PRIMARYSECTORNAME')[
                    'MARKETVALUEWITHOUTACCRUEDINCOME'].sum().reset_index()
                # print(temp_df)
                temp_df.columns = ['PRIMARYSECTORNAME', 'MARKETVALUEWITHOUTACCRUEDINCOME']
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                temp_df['SECTORSCHEME'] = 'GICS Sector'
                temp_df['CURRENCYCODE'] = 'USD'
                temp_df['CURRENCY'] = 'US Dollar'
                temp_df['LANGUAGECODE'] = 'en-US'
                temp_df['CATEGORY'] = 'Sector'
                temp_df['MARKETVALUE'] = temp_df['MARKETVALUEWITHOUTACCRUEDINCOME']
                temp_df = temp_df.rename({'PRIMARYSECTORNAME': 'CATEGORYNAME'}, axis=1)
                temp_df['PORTFOLIOWEIGHT'] = np.nan
                temp_df['PORTFOLIOWEIGHT'] = 100 * temp_df['MARKETVALUEWITHOUTACCRUEDINCOME'] / \
                                                       temp_df['MARKETVALUEWITHOUTACCRUEDINCOME'].sum()
                temp_df['INDEX1WEIGHT'] = np.nan
                temp_df['INDEX2WEIGHT'] = np.nan
                temp_df['INDEX3WEIGHT'] = np.nan
                temp_df['ABBREVIATEDTEXT'] = np.nan

                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM SECTORALLOCATION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        for index_inner, row_inner in temp_df.iterrows():
                            temp_df.at[index_inner, 'INDEX1WEIGHT'] = float(temp_df.at[
                                                                          index_inner, 'PORTFOLIOWEIGHT']) + float(
                                np.random.uniform(-1, 1))
                            temp_df.at[index_inner, 'INDEX2WEIGHT'] = float(temp_df.at[index_inner, 'INDEX1WEIGHT']) + float(
                                np.random.uniform(0, 1))
                            temp_df.at[index_inner, 'INDEX3WEIGHT'] = float(temp_df.at[index_inner, 'INDEX1WEIGHT']) + float(
                                np.random.uniform(-1, 0))
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX2WEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX3WEIGHT', 100)
                        temp_df = temp_df[
                            ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'LANGUAGECODE',
                             'MARKETVALUEWITHOUTACCRUEDINCOME', 'MARKETVALUE', 'SECTORSCHEME', 'CATEGORY',
                             'CATEGORYNAME',
                             'PORTFOLIOWEIGHT', 'INDEX1WEIGHT', 'INDEX2WEIGHT', 'INDEX3WEIGHT', 'ABBREVIATEDTEXT']]
                        result_df = pd.concat([result_df, temp_df])
                else:
                    for index_inner, row_inner in temp_df.iterrows():
                        temp_df.at[index_inner, 'INDEX1WEIGHT'] = temp_df.at[index_inner, 'PORTFOLIOWEIGHT'] + float(
                            np.random.uniform(-1, 1))
                        temp_df.at[index_inner, 'INDEX2WEIGHT'] = temp_df.at[index_inner, 'INDEX1WEIGHT'] + float(
                            np.random.uniform(0, 1))
                        temp_df.at[index_inner, 'INDEX3WEIGHT'] = temp_df.at[index_inner, 'INDEX1WEIGHT'] + float(
                            np.random.uniform(-1, 0))
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX2WEIGHT', 100)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX3WEIGHT', 100)
                    temp_df = temp_df[
                        ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'LANGUAGECODE',
                         'MARKETVALUEWITHOUTACCRUEDINCOME', 'MARKETVALUE', 'SECTORSCHEME', 'CATEGORY', 'CATEGORYNAME',
                         'PORTFOLIOWEIGHT', 'INDEX1WEIGHT', 'INDEX2WEIGHT', 'INDEX3WEIGHT', 'ABBREVIATEDTEXT']]
                    result_df = pd.concat([result_df, temp_df])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        
        return result_df

    def create_region_allocation_data(self):
        # NOTE : The region allocation data will depend on the holdings' data. Make sure to generate the synthetic holdings data before generating the region allocation data
        general_info_df = self.portfolio_general_info_df
        result_df = pd.DataFrame()
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM REGIONALLOCATION WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM REGIONALLOCATION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                sample_data_info = self.conn.cursor().execute(
                    f"select PORTFOLIOCODE, REGIONCLASSIFICATIONSCHEME, REGIONNAME, MARKETVALUEWITHOUTACCRUEDINCOME, MARKETVALUE, HISTORYDATE from  HOLDINGSDETAILS where PORTFOLIOCODE='"+portfolio_code+"' and HISTORYDATE = '"+as_of_date+"'")
                holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                    columns=[x[0] for x in
                                                                             sample_data_info.description])
                # print(holdings_sample_data_df)
                temp_df = holdings_sample_data_df.copy(deep=True)
                temp_df = temp_df.groupby('REGIONNAME')[
                    'MARKETVALUEWITHOUTACCRUEDINCOME'].sum().reset_index()
                temp_df.columns = ['REGIONNAME', 'MARKETVALUEWITHOUTACCRUEDINCOME']
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                temp_df['CURRENCYCODE'] = 'USD'
                temp_df['CURRENCY'] = 'US Dollar'
                temp_df['LANGUAGECODE'] = 'en-US'
                temp_df['REGIONSCHEME'] = 'MSCI Regions'
                temp_df['MARKETVALUE'] = temp_df['MARKETVALUEWITHOUTACCRUEDINCOME']
                temp_df = temp_df.rename({'REGIONNAME': 'REGION'}, axis=1)
                temp_df['PORTFOLIOWEIGHT'] = np.nan
                temp_df['PORTFOLIOWEIGHT'] = 100 * temp_df['MARKETVALUEWITHOUTACCRUEDINCOME'] / \
                                             temp_df['MARKETVALUEWITHOUTACCRUEDINCOME'].sum()
                temp_df['INDEX1WEIGHT'] = np.nan
                temp_df['INDEX2WEIGHT'] = np.nan
                temp_df['INDEX3WEIGHT'] = np.nan
                temp_df['ABBREVIATEDTEXT'] = np.nan
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM REGIONALLOCATION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        for index_inner, row_inner in temp_df.iterrows():
                            temp_df.at[index_inner, 'INDEX1WEIGHT'] = float(temp_df.at[
                                                                          index_inner, 'PORTFOLIOWEIGHT']) + float(
                                np.random.uniform(-1.5, 1.5))
                            temp_df.at[index_inner, 'INDEX2WEIGHT'] = float(temp_df.at[
                                                                          index_inner, 'PORTFOLIOWEIGHT']) + float(
                                np.random.uniform(-1.5, 1.5))
                            temp_df.at[index_inner, 'INDEX3WEIGHT'] = float(temp_df.at[
                                                                          index_inner, 'INDEX1WEIGHT']) + float(
                                np.random.uniform(-0.3, 0.3))
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX2WEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX3WEIGHT', 100)
                        temp_df = temp_df[
                            ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'LANGUAGECODE',
                             'MARKETVALUEWITHOUTACCRUEDINCOME', 'MARKETVALUE', 'REGIONSCHEME', 'REGION',
                             'PORTFOLIOWEIGHT', 'INDEX1WEIGHT', 'INDEX2WEIGHT', 'INDEX3WEIGHT', 'ABBREVIATEDTEXT']]
                        result_df = pd.concat([result_df, temp_df])
                else:
                    for index_inner, row_inner in temp_df.iterrows():
                        temp_df.at[index_inner, 'INDEX1WEIGHT'] = float(temp_df.at[
                                                                      index_inner, 'PORTFOLIOWEIGHT']) + float(
                            np.random.uniform(-1.5, 1.5))
                        temp_df.at[index_inner, 'INDEX2WEIGHT'] = float(temp_df.at[
                                                                      index_inner, 'PORTFOLIOWEIGHT']) + float(
                            np.random.uniform(-1.5, 1.5))
                        temp_df.at[index_inner, 'INDEX3WEIGHT'] = float(temp_df.at[index_inner, 'INDEX1WEIGHT']) + float(
                            np.random.uniform(-0.3, 0.3))
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX2WEIGHT', 100)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX3WEIGHT', 100)
                    temp_df = temp_df[
                        ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'LANGUAGECODE',
                         'MARKETVALUEWITHOUTACCRUEDINCOME', 'MARKETVALUE', 'REGIONSCHEME', 'REGION',
                         'PORTFOLIOWEIGHT', 'INDEX1WEIGHT', 'INDEX2WEIGHT', 'INDEX3WEIGHT', 'ABBREVIATEDTEXT']]
                    result_df = pd.concat([result_df, temp_df])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)

        return result_df


    def create_attribution_data(self):
        general_info_df = self.portfolio_general_info_df
        portfolio_codes = general_info_df['PORTFOLIOCODE'].unique()
        np.random.shuffle(portfolio_codes)
        split_index = int(len(portfolio_codes) * 0.8)
        rep_account_code_list = portfolio_codes[:split_index].tolist()

        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM ATTRIBUTION WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"select * from ATTRIBUTION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        attribution_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])


        random_value_ranges_rep_accounts = { "ACCOUNTAVERAGEWEIGHT":(-0.05,0.05), "BENCHMARKAVERAGEWEIGHT": (-0.01,0.01), "ACCOUNTTOTALRETURN":(-0.1,0.1), "BENCHMARKTOTALRETURN":(-0.08,0.08)}
        random_value_ranges_non_rep_accounts = { "ACCOUNTAVERAGEWEIGHT":(-1.5,1.5), "BENCHMARKAVERAGEWEIGHT": (-1,1), "ACCOUNTTOTALRETURN":(-0.6,0.6), "BENCHMARKTOTALRETURN":(-0.4,0.4)}
        non_negative_fields = ["ACCOUNTAVERAGEWEIGHT", "BENCHMARKAVERAGEWEIGHT"]
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM ATTRIBUTION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                random_value_ranges = dict()
                if (portfolio_code in rep_account_code_list) or (portfolio_code == self.base_portfolio_code):
                    random_value_ranges = random_value_ranges_rep_accounts
                else:
                    random_value_ranges = random_value_ranges_non_rep_accounts
                print("Generating attribution data for: ", portfolio_code)
                temp_df = attribution_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                primary_categories = temp_df['CATEGORY'].unique()
                category_schemes = temp_df['CATEGORYSCHEME'].unique()

                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM ATTRIBUTION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        if 'Total' in primary_categories:
                            primary_categories = primary_categories[primary_categories != 'Total']
                        secondary_categories = temp_df['SECONDARYCATEGORY'].unique()
                        tertiary_categories = temp_df['TERTIARYCATEGORY'].unique()
                        benchmarks = temp_df['BENCHMARKCODE'].unique()
                        periods = temp_df['PERIOD'].unique()
                        currency_codes = temp_df['CURRENCYCODE'].unique()
                        benchmark_total_return = 0

                        # Addressing Total category first
                        # total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total')&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                        total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total') &
                                (pd.isna(temp_df["SECONDARYCATEGORY"])) &
                                (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                        # print(total_category_df)
                        for category_scheme in category_schemes:
                            category_scheme_df = total_category_df[(total_category_df['CATEGORYSCHEME'] == category_scheme)]
                            for benchmark in benchmarks:
                                benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                for period in periods:
                                    period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                    for currency_code in currency_codes:
                                        currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                        currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                        currency_df['ACCOUNTAVERAGEWEIGHT'] = 100
                                        currency_df['BENCHMARKAVERAGEWEIGHT']= 100
                                        result_df = pd.concat([result_df, currency_df])
                                        # print(result_df)

                        #Addressing primary categories
                        for primary_category in primary_categories:
                            # primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                            primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category) &
                                (pd.isna(temp_df["SECONDARYCATEGORY"])) &
                                (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                            for category_scheme in category_schemes:
                                category_scheme_df = primary_category_df[(primary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                for benchmark in benchmarks:
                                    benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                    for period in periods:
                                        period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                        for currency_code in currency_codes:
                                            currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                            currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                            currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                            currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                            try:
                                                benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                            except Exception as err:
                                                print(portfolio_code, benchmark, period, currency_code, category_scheme, primary_category)
                                                raise ValueError(f"Error: {err}")
                                            currency_df["ALLOCATIONEFFECT"] =  ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                            currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                            currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                            currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                            currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                            result_df = pd.concat([result_df, currency_df])

                        #Addressing secondary categories
                        for primary_category in primary_categories:
                            for secondary_category in secondary_categories:
                                # secondary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == secondary_category)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                                secondary_category_df = temp_df[(temp_df['CATEGORY']==primary_category) &
                                                            (temp_df["SECONDARYCATEGORY"] == secondary_category) &
                                                            (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                                for category_scheme in category_schemes:
                                    category_scheme_df = secondary_category_df[(secondary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                    for benchmark in benchmarks:
                                        benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                        for period in periods:
                                            period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                            for currency_code in currency_codes:
                                                currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                                currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                                currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                                currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                                benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                                currency_df["ALLOCATIONEFFECT"] = ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                                currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                                currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                                currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                                currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                                result_df = pd.concat([result_df, currency_df])


                        #Addressing tertiary categories
                        for primary_category in primary_categories:
                            for secondary_category in secondary_categories:
                                for tertiary_category in tertiary_categories:
                                    # tertiary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == tertiary_category)]
                                    tertiary_category_df = temp_df[(temp_df['CATEGORY']==primary_category) &
                                                            (temp_df["SECONDARYCATEGORY"] == secondary_category) &
                                                            (temp_df["TERTIARYCATEGORY"] == tertiary_category)]
                                    for category_scheme in category_schemes:
                                        category_scheme_df = tertiary_category_df[(tertiary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                        for benchmark in benchmarks:
                                            benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                            for period in periods:
                                                period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                                for currency_code in currency_codes:
                                                    currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                                    currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                                    currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                                    currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                                    benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                                    currency_df["ALLOCATIONEFFECT"] = ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                                    currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                                    currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                                    currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                                    currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                                    result_df = pd.concat([result_df, currency_df])
                else:
                    if 'Total' in primary_categories:
                        primary_categories = primary_categories[primary_categories != 'Total']
                    secondary_categories = temp_df['SECONDARYCATEGORY'].unique()
                    tertiary_categories = temp_df['TERTIARYCATEGORY'].unique()
                    benchmarks = temp_df['BENCHMARKCODE'].unique()
                    periods = temp_df['PERIOD'].unique()
                    currency_codes = temp_df['CURRENCYCODE'].unique()
                    benchmark_total_return = 0

                    # Addressing Total category first
                    # total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total')&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                    total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total') &
                                                (pd.isna(temp_df["SECONDARYCATEGORY"])) &
                                                (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                    # print(total_category_df)
                    for category_scheme in category_schemes:
                        category_scheme_df = total_category_df[(total_category_df['CATEGORYSCHEME'] == category_scheme)]
                        for benchmark in benchmarks:
                            benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                            for period in periods:
                                period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                for currency_code in currency_codes:
                                    currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                    currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                        non_negative_fields)
                                    currency_df['ACCOUNTAVERAGEWEIGHT'] = 100
                                    currency_df['BENCHMARKAVERAGEWEIGHT'] = 100
                                    result_df = pd.concat([result_df, currency_df])
                                    # print(result_df)

                    # Addressing primary categories
                    for primary_category in primary_categories:
                        # primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                        primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category) &
                                                      (pd.isna(temp_df["SECONDARYCATEGORY"])) &
                                                      (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                        for category_scheme in category_schemes:
                            category_scheme_df = primary_category_df[
                                (primary_category_df['CATEGORYSCHEME'] == category_scheme)]
                            for benchmark in benchmarks:
                                benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                for period in periods:
                                    period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                    for currency_code in currency_codes:
                                        currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                        currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                            non_negative_fields)
                                        currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                        currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                        try:
                                            benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                        except Exception as err:
                                            print(portfolio_code, benchmark, period, currency_code, primary_category, category_scheme)
                                            result_df.to_csv("investigate_errors.csv", sep="|")
                                            raise IndexError(f"Error: {err}")
                                        currency_df["ALLOCATIONEFFECT"] = ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                        currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )                                        
                                        currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                        currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - \
                                                                                currency_df["BENCHMARKAVERAGEWEIGHT"]
                                        currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - \
                                                                              currency_df["BENCHMARKTOTALRETURN"]
                                        result_df = pd.concat([result_df, currency_df])

                    # Addressing secondary categories
                    for primary_category in primary_categories:
                        for secondary_category in secondary_categories:
                            # secondary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == secondary_category)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
                            secondary_category_df = temp_df[(temp_df["CATEGORY"] == primary_category)&
                                                            (temp_df["SECONDARYCATEGORY"] == secondary_category) &
                                                            (pd.isna(temp_df["TERTIARYCATEGORY"]))]
                            for category_scheme in category_schemes:
                                category_scheme_df = secondary_category_df[
                                    (secondary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                for benchmark in benchmarks:
                                    benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                    for period in periods:
                                        period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                        for currency_code in currency_codes:
                                            currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                            currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                                non_negative_fields)
                                            currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                            currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                            benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark) & (
                                                        result_df['PERIOD'] == period) & (result_df[
                                                                                            'CURRENCYCODE'] == currency_code)][
                                                'BENCHMARKTOTALRETURN'].iloc[0]
                                            currency_df["ALLOCATIONEFFECT"] = ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                            currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                            
                                            currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df[
                                                "SELECTIONANDINTERACTIONEFFECT"]
                                            currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - \
                                                                                    currency_df["BENCHMARKAVERAGEWEIGHT"]
                                            currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - \
                                                                                currency_df["BENCHMARKTOTALRETURN"]
                                            result_df = pd.concat([result_df, currency_df])

                    # Addressing tertiary categories
                    for primary_category in primary_categories:
                        for secondary_category in secondary_categories:
                            for tertiary_category in tertiary_categories:
                                # tertiary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == tertiary_category)]
                                tertiary_category_df = temp_df[(temp_df["CATEGORY"] == primary_category)&
                                                            (temp_df["SECONDARYCATEGORY"] == secondary_category) &
                                                            (temp_df["TERTIARYCATEGORY"] == tertiary_category)]
                                
                                for category_scheme in category_schemes:
                                    category_scheme_df = tertiary_category_df[
                                        (tertiary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                    for benchmark in benchmarks:
                                        benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                        for period in periods:
                                            period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                            for currency_code in currency_codes:
                                                currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                                currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                                    non_negative_fields)
                                                currency_df = adjust_sum_of_column(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                                currency_df = adjust_sum_of_column(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                                benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark) & (
                                                            result_df['PERIOD'] == period) & (result_df[
                                                                                                'CURRENCYCODE'] == currency_code)][
                                                    'BENCHMARKTOTALRETURN'].iloc[0]
                                                currency_df["ALLOCATIONEFFECT"] = ((currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"])/100) * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                                currency_df["SELECTIONANDINTERACTIONEFFECT"] = (currency_df["BENCHMARKAVERAGEWEIGHT"]/100) * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                                currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df[
                                                    "SELECTIONANDINTERACTIONEFFECT"]
                                                currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - \
                                                                                        currency_df["BENCHMARKAVERAGEWEIGHT"]
                                                currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - \
                                                                                    currency_df["BENCHMARKTOTALRETURN"]
                                                result_df = pd.concat([result_df, currency_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                attribution_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from ATTRIBUTION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                attribution_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in
                                                                                                        sample_data_info.description])

        return result_df
    # one common warning for attribution
    # RuntimeWarning: divide by zero encountered in scalar divide
    # reassign_value = misaligned_sum / non_zero_rows


    def create_transactions_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        #no need to remove duplicates for transactions
        while True:
            query_template = f"SELECT COUNT(*) FROM TRANSACTIONDETAILS WHERE SETTLEDATE > TO_TIMESTAMP ('{self.prior_month_end(base_as_of_date)}')"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"WITH FilteredTransactions AS (SELECT * FROM TRANSACTIONDETAILS WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' AND SETTLEDATE > '{self.prior_month_end(base_as_of_date)}' AND SETTLEDATE <= '{base_as_of_date}' AND QUANTITY is not null AND BASECOMMISSIONVALUE is not null), "
                                                      f"RankedTransactions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ISSUESYMBOL ORDER BY PRICE) AS rn FROM FilteredTransactions)"
                                                      f"SELECT * FROM RankedTransactions WHERE rn = 1 ORDER BY PRICE OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;")
        transactions_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        # Drop the 'RN' column
        transactions_sample_data_df.drop(columns=['RN'], inplace=True)
        random_value_ranges = {"QUANTITY":(-100,100), "PRICE": (-10,10), "BASECOMMISSIONVALUE": (-1,1)}
        non_negative_fields = ["QUANTITY", "PRICE", "BASECOMMISSIONVALUE"]

        result_df = pd.DataFrame()
        base_as_of_date = self.base_as_of_date
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            # may can't remove all duplicates for transactions
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM TRANSACTIONDETAILS WHERE SETTLEDATE > TO_TIMESTAMP('{base_as_of_date}') GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = transactions_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM TRANSACTIONDETAILS WHERE PORTFOLIOCODE = '{portfolio_code}' and SETTLEDATE > TO_TIMESTAMP('{base_as_of_date}')")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        temp_df['TOTALBASEVALUE']  = temp_df['QUANTITY'] * temp_df['PRICE']
                        temp_df['TOTALLOCALVALUE'] = temp_df['TOTALBASEVALUE']
                        temp_df['LOCALCOMMISSIONVALUE'] = temp_df['BASECOMMISSIONVALUE']
                        # generate random date for the time range for settledate
                        date_range = pd.date_range(start=base_as_of_date, end=as_of_date)
                        temp_df['SETTLEDATE'] = np.random.choice(date_range, size=len(temp_df))
                        # Set up TRADEDATE as random 2 to 5 days prior to SETTLEDATE
                        temp_df['TRADEDATE'] = temp_df['SETTLEDATE'] - pd.to_timedelta(np.random.randint(2, 6, size=len(temp_df)), unit='d')
                        # Randomly assign "Sell" or "Buy" to TRANSACTIONTYPENAME
                        temp_df['TRANSACTIONTYPENAME'] = np.random.choice(['Sell', 'Buy'], size=len(temp_df))
                        # Set TRANSACTIONTYPECODE based on TRANSACTIONTYPENAME
                        temp_df['TRANSACTIONTYPECODE'] = temp_df['TRANSACTIONTYPENAME'].apply(lambda x: 'sl' if x == 'Sell' else 'by')
                        # Apply the function to each row in the DataFrame
                        temp_df = temp_df.apply(self.assign_broker, axis=1)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    temp_df['TOTALBASEVALUE'] = temp_df['QUANTITY'] * temp_df['PRICE']
                    temp_df['TOTALLOCALVALUE'] = temp_df['TOTALBASEVALUE']
                    temp_df['LOCALCOMMISSIONVALUE'] = temp_df['BASECOMMISSIONVALUE']
                    # generate random date for the time range for settledate
                    date_range = pd.date_range(start=base_as_of_date, end=as_of_date)
                    temp_df['SETTLEDATE'] = np.random.choice(date_range, size=len(temp_df))
                    # Set up TRADEDATE as random 2 to 5 days prior to SETTLEDATE
                    temp_df['TRADEDATE'] = temp_df['SETTLEDATE'] - pd.to_timedelta(
                        np.random.randint(2, 6, size=len(temp_df)), unit='d')
                    # Randomly assign "Sell" or "Buy" to TRANSACTIONTYPENAME
                    temp_df['TRANSACTIONTYPENAME'] = np.random.choice(['Sell', 'Buy'], size=len(temp_df))
                    # Set TRANSACTIONTYPECODE based on TRANSACTIONTYPENAME
                    temp_df['TRANSACTIONTYPECODE'] = temp_df['TRANSACTIONTYPENAME'].apply(
                        lambda x: 'sl' if x == 'Sell' else 'by')
                    # Apply the function to each row in the DataFrame
                    temp_df = temp_df.apply(self.assign_broker, axis=1)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            try:
                transactions_sample_data_df = result_df[
                    (result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (result_df['SETTLEDATE'] > base_as_of_date) & (
                                result_df['SETTLEDATE'] <= as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"WITH FilteredTransactions AS (SELECT * FROM TRANSACTIONDETAILS WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' AND SETTLEDATE > '{base_as_of_date}' AND SETTLEDATE <= '{as_of_date}' AND QUANTITY is not null AND BASECOMMISSIONVALUE is not null), "
                    f"RankedTransactions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ISSUESYMBOL ORDER BY PRICE) AS rn FROM FilteredTransactions)"
                    f"SELECT * FROM RankedTransactions WHERE rn = 1 ORDER BY PRICE OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;")
                transactions_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in
                                                                                                         sample_data_info.description])
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)

        return result_df


    def create_portfolio_characteristics_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM PORTFOLIOCHARACTERISTICS WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' AND HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select * from PORTFOLIOCHARACTERISTICS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        port_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                            columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"CHARACTERISTICVALUE": (-0.5, 0.5)}
        non_negative_fields = ["CHARACTERISTICVALUE"]

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM PORTFOLIOCHARACTERISTICS WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = port_characteristics_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM PORTFOLIOCHARACTERISTICS WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                port_characteristics_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from PORTFOLIOCHARACTERISTICS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                port_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                columns=[x[0] for x in
                                                                                         sample_data_info.description])

        return result_df


    def create_benchmark_characteristics_data(self):
        general_info = self.conn.cursor().execute(f"SELECT DISTINCT(b.BENCHMARKCODE) FROM PORTFOLIOBENCHMARKASSOCIATION b "
                                                  f"INNER JOIN PORTFOLIOATTRIBUTES p ON p.PORTFOLIOCODE = b.PORTFOLIOCODE "
                                                  f"WHERE p.ATTRIBUTETYPECODE = '{self.strategy_code}'")
        general_info_df = pd.DataFrame.from_records(iter(general_info),
                                                    columns=[x[0] for x in general_info.description])
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            # Check if the result/count is greater than 0
            if result > 0:
                break
            # Update the base_as_of_date to the prior month end
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select DISTINCT * from BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
        bench_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                            columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"CHARACTERISTICVALUE": (-0.5, 0.5)}
        non_negative_fields = ["CHARACTERISTICVALUE"]

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT BENCHMARKCODE, COUNT(*) FROM BENCHMARKCHARACTERISTICS WHERE HISTORYDATE = '{as_of_date}' GROUP BY BENCHMARKCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                benchmark_code = row['BENCHMARKCODE']
                temp_df = bench_characteristics_sample_data_df.copy(deep=True)
                temp_df['BENCHMARKCODE'] = benchmark_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == benchmark_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM BENCHMARKCHARACTERISTICS WHERE BENCHMARKCODE = '{benchmark_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                bench_characteristics_sample_data_df = result_df[(result_df['BENCHMARKCODE'] == self.base_benchmark_code) & (
                    result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select DISTINCT * from BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
                bench_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                 columns=[x[0] for x in
                                                                                          sample_data_info.description])

        return result_df


    def create_performance_factors(self):
        general_info_df = self.portfolio_general_info_df
        performance_factor_columns = ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'PERFORMANCECATEGORY',
                                      'PERFORMANCECATEGORYNAME', 'PERFORMANCETYPE', 'PERFORMANCEINCEPTIONDATE',
                                      'PORTFOLIOINCEPTIONDATE', 'PERFORMANCEFREQUENCY', 'PERFORMANCEFACTOR']
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM PORTFOLIOPERFORMANCE WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            # Check if the result/count is greater than 0
            if result > 0:
                break
            # Update the base_as_of_date to the prior month end
            base_as_of_date = self.prior_month_end(base_as_of_date)
        # print("Final base as of date is: ", base_as_of_date)
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        result_df = pd.DataFrame()

        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM PORTFOLIOPERFORMANCE WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE"
            ).fetchall()
            for row_index, row in general_info_df.iterrows():
                temp_df = pd.DataFrame(columns=performance_factor_columns)
                portfolio_code = row['PORTFOLIOCODE']
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM PORTFOLIOPERFORMANCE WHERE PORTFOLIOCODE = '{portfolio_code}' AND HISTORYDATE > '{base_as_of_date}' AND HISTORYDATE <= '{as_of_date}'"
                        )
                        performance_inception_date = row['PERFORMANCEINCEPTIONDATE']
                        last_performance_factor_date_query = f"SELECT MAX(HISTORYDATE) FROM portfolioperformance WHERE PORTFOLIOCODE = '{portfolio_code}' AND HISTORYDATE <= '{base_as_of_date}'"
                        last_performance_factor_date = \
                        self.conn.cursor().execute(last_performance_factor_date_query).fetchone()[0]
                        if last_performance_factor_date is None:
                            last_performance_factor_date = performance_inception_date
                        else:
                            last_performance_factor_date = (
                                    pd.to_datetime(last_performance_factor_date) + pd.Timedelta(days=1)
                            ).strftime('%Y-%m-%d %H:%M:%S.%f')

                        date_range = pd.date_range(start=last_performance_factor_date, end=as_of_date, freq='D')
                        temp_df['HISTORYDATE'] = date_range
                        temp_df['PORTFOLIOCODE'] = portfolio_code
                        temp_df['CURRENCYCODE'] = row['BASECURRENCYCODE']
                        temp_df['CURRENCY'] = row['BASECURRENCYNAME']
                        temp_df['PERFORMANCECATEGORY'] = 'Asset Class'
                        temp_df['PERFORMANCECATEGORYNAME'] = 'Total Portfolio'
                        temp_df['PERFORMANCETYPE'] = 'Portfolio Gross'
                        temp_df['PERFORMANCEINCEPTIONDATE'] = performance_inception_date
                        temp_df['PORTFOLIOINCEPTIONDATE'] = row['OPENDATE']
                        temp_df['PERFORMANCEFREQUENCY'] = 'D'
                        temp_df['PERFORMANCEFACTOR'] = np.random.normal(0.008, 0.01, len(date_range))
                        temp_df_net = temp_df.copy()
                        temp_df_net['PERFORMANCETYPE'] = 'Portfolio Net'
                        temp_df_net['PERFORMANCEFACTOR'] = temp_df['PERFORMANCEFACTOR'] - 0.0025
                        result_df = pd.concat([result_df, temp_df])
                        result_df = pd.concat([result_df, temp_df_net])
                else:
                    performance_inception_date = row['PERFORMANCEINCEPTIONDATE']
                    last_performance_factor_date_query = f"SELECT MAX(HISTORYDATE) FROM portfolioperformance WHERE PORTFOLIOCODE = '{portfolio_code}' AND HISTORYDATE <= '{base_as_of_date}'"
                    last_performance_factor_date = \
                    self.conn.cursor().execute(last_performance_factor_date_query).fetchone()[0]
                    if last_performance_factor_date is None:
                        last_performance_factor_date = performance_inception_date
                    else:
                        last_performance_factor_date = (
                                pd.to_datetime(last_performance_factor_date) + pd.Timedelta(days=1)
                        ).strftime('%Y-%m-%d %H:%M:%S.%f')

                    date_range = pd.date_range(start=last_performance_factor_date, end=as_of_date, freq='D')
                    temp_df['HISTORYDATE'] = date_range
                    temp_df['PORTFOLIOCODE'] = portfolio_code
                    temp_df['CURRENCYCODE'] = row['BASECURRENCYCODE']
                    temp_df['CURRENCY'] = row['BASECURRENCYNAME']
                    temp_df['PERFORMANCECATEGORY'] = 'Asset Class'
                    temp_df['PERFORMANCECATEGORYNAME'] = 'Total Portfolio'
                    temp_df['PERFORMANCETYPE'] = 'Portfolio Gross'
                    temp_df['PERFORMANCEINCEPTIONDATE'] = performance_inception_date
                    temp_df['PORTFOLIOINCEPTIONDATE'] = row['OPENDATE']
                    temp_df['PERFORMANCEFREQUENCY'] = 'D'
                    temp_df['PERFORMANCEFACTOR'] = np.random.normal(0.008, 0.01, len(date_range))
                    temp_df_net = temp_df.copy()
                    temp_df_net['PERFORMANCETYPE'] = 'Portfolio Net'
                    temp_df_net['PERFORMANCEFACTOR'] = temp_df['PERFORMANCEFACTOR'] - 0.0025
                    result_df = pd.concat([result_df, temp_df])
                    result_df = pd.concat([result_df, temp_df_net])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)

        return result_df

    def create_benchmark_prices(self):
        performance_factor_columns = ['BENCHMARKCODE', 'PERFORMANCEDATATYPE', 'CURRENCYCODE', 'CURRENCY',
                                      'PERFORMANCEFREQUENCY', 'VALUE', 'HISTORYDATE']
        result_df = pd.DataFrame()
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM BENCHMARKPERFORMANCE where BENCHMARKCODE = '{self.base_benchmark_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            # Check if the result/count is greater than 0
            if result > 0:
                break
            # Update the base_as_of_date to the prior month end
            base_as_of_date = self.prior_month_end(base_as_of_date)
        unique_benchmark_query_result = self.conn.cursor().execute(
            f"SELECT DISTINCT(b.BENCHMARKCODE) FROM PORTFOLIOBENCHMARKASSOCIATION b "
            f"INNER JOIN PORTFOLIOATTRIBUTES p ON p.PORTFOLIOCODE = b.PORTFOLIOCODE "
            f"WHERE p.ATTRIBUTETYPECODE = '{self.strategy_code}'")
        unique_benchmark_info_df = pd.DataFrame.from_records(iter(unique_benchmark_query_result), columns=[x[0] for x in
                                                                                                           unique_benchmark_query_result.description])
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT BENCHMARKCODE, COUNT(*) FROM BENCHMARKPERFORMANCE WHERE HISTORYDATE = '{as_of_date}' GROUP BY BENCHMARKCODE"
            ).fetchall()
            for row_index, row in unique_benchmark_info_df.iterrows():
                temp_df = pd.DataFrame(columns=performance_factor_columns)
                benchmark_code = row['BENCHMARKCODE']
                if any(item[0] == benchmark_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM PORTFOLIOPERFORMANCE WHERE BENCHMARKCODE = '{benchmark_code}' AND HISTORYDATE > '{base_as_of_date}' AND HISTORYDATE <= '{as_of_date}'"
                        )
                        default_inception_date = '2000-01-01'
                        last_price_date_query = f"SELECT max(HISTORYDATE) FROM benchmarkperformance WHERE BENCHMARKCODE = '{benchmark_code}' AND HISTORYDATE <= '{base_as_of_date}'"
                        last_price_date = self.conn.cursor().execute(last_price_date_query).fetchone()[0]

                        last_price = 100

                        if last_price_date is None:
                            last_price_date = default_inception_date
                            last_price = 100
                        else:
                            last_price = self.conn.cursor().execute(
                                f"SELECT VALUE FROM benchmarkperformance WHERE BENCHMARKCODE = '{benchmark_code}' AND HISTORYDATE = '{last_price_date}'").fetchone()[
                                0]
                            last_price_date = (pd.to_datetime(last_price_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

                        date_range = pd.date_range(start=last_price_date, end=as_of_date, freq='D')
                        temp_df['HISTORYDATE'] = date_range
                        temp_df['BENCHMARKCODE'] = benchmark_code
                        temp_df['CURRENCYCODE'] = 'USD'
                        temp_df['CURRENCY'] = 'US Dollar'
                        temp_df['PERFORMANCEDATATYPE'] = 'Prices'
                        temp_df['PERFORMANCEFREQUENCY'] = np.nan
                        temp_df['VALUE'] = np.nan
                        for i in range(len(temp_df)):
                            if i == 0:
                                temp_df.at[i, 'VALUE'] = last_price * (1 + np.random.normal(0.00018, 0.0001))
                            else:
                                temp_df.at[i, 'VALUE'] = temp_df.at[i - 1, 'VALUE'] * (1 + np.random.normal(0.00018, 0.0001))
                        result_df = pd.concat([result_df, temp_df])
                else:
                    default_inception_date = '2000-01-01'
                    last_price_date_query = f"SELECT max(HISTORYDATE) FROM benchmarkperformance WHERE BENCHMARKCODE = '{benchmark_code}' AND HISTORYDATE <= '{base_as_of_date}'"
                    last_price_date = self.conn.cursor().execute(last_price_date_query).fetchone()[0]

                    last_price = 100

                    if last_price_date is None:
                        last_price_date = default_inception_date
                        last_price = 100
                    else:
                        last_price = self.conn.cursor().execute(
                            f"SELECT VALUE FROM benchmarkperformance WHERE BENCHMARKCODE = '{benchmark_code}' AND HISTORYDATE = '{last_price_date}'").fetchone()[
                            0]
                        last_price_date = (pd.to_datetime(last_price_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

                    date_range = pd.date_range(start=last_price_date, end=as_of_date, freq='D')
                    temp_df['HISTORYDATE'] = date_range
                    temp_df['BENCHMARKCODE'] = benchmark_code
                    temp_df['CURRENCYCODE'] = 'USD'
                    temp_df['CURRENCY'] = 'US Dollar'
                    temp_df['PERFORMANCEDATATYPE'] = 'Prices'
                    temp_df['PERFORMANCEFREQUENCY'] = np.nan
                    temp_df['VALUE'] = np.nan
                    for i in range(len(temp_df)):
                        if i == 0:
                            temp_df.at[i, 'VALUE'] = last_price * (1 + np.random.normal(0.001, 0.001))
                        else:
                            temp_df.at[i, 'VALUE'] = temp_df.at[i - 1, 'VALUE'] * (1 + np.random.normal(0.001, 0.001))
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)

        return result_df

    def create_credit_quality_allocation_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM CREDITQUALITYALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            # Check if the result/count is greater than 0
            if result > 0:
                break
            # Update the base_as_of_date to the prior month end
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select * from CREDITQUALITYALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        quality_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                           columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"PORTFOLIOWEIGHT": (-0.5, 0.5), "INDEX1WEIGHT": (-0.5, 0.5)}
        non_negative_fields = ["PORTFOLIOWEIGHT", "INDEX1WEIGHT"]
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM CREDITQUALITYALLOCATION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE"
            ).fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = quality_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM CREDITQUALITYALLOCATION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df= add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'PORTFOLIOWEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'PORTFOLIOWEIGHT', 100)
                    temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                    result_df = pd.concat([result_df, temp_df])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                quality_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from CREDITQUALITYALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                quality_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                   columns=[x[0] for x in sample_data_info.description])


        return result_df

    def create_duration_allocation_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM DURATIONALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select * from DURATIONALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        duration_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                            columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"PORTFOLIOWEIGHT": (-0.5, 0.5), "INDEX1WEIGHT": (-0.5, 0.5)}
        non_negative_fields = ["PORTFOLIOWEIGHT", "INDEX1WEIGHT"]
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM DURATIONALLOCATION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE"
            ).fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = duration_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM DURATIONALLOCATION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        # Group by PORTFOLIOCODE and DURATION
                        grouped = temp_df.groupby(['PORTFOLIOCODE', 'DURATIONTYPE'])

                        for (portfolio, duration), group in grouped:
                            temp_df = group.copy(deep=True)
                            temp_df = adjust_sum_of_columns_no_negative(temp_df, 'PORTFOLIOWEIGHT', 100)
                            temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                            result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    # Group by PORTFOLIOCODE and DURATION
                    grouped = temp_df.groupby(['PORTFOLIOCODE', 'DURATIONTYPE'])

                    for (portfolio, duration), group in grouped:
                        temp_df = group.copy(deep=True)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'PORTFOLIOWEIGHT', 100)
                        temp_df = adjust_sum_of_columns_no_negative(temp_df, 'INDEX1WEIGHT', 100)
                        result_df = pd.concat([result_df, temp_df])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                duration_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from DURATIONALLOCATION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                duration_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                    columns=[x[0] for x in
                                                                             sample_data_info.description])


        return result_df


    def create_fixed_income_attribution_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM FIXEDINCOMEATTRIBUTION WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(f"select * from FIXEDINCOMEATTRIBUTION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        fi_attribution_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = { "ACCOUNTAVERAGEWEIGHT":(-5,5), "BENCHMARKAVERAGEWEIGHT": (-5,5), "ACCOUNTTOTALRETURN":(-2.5,2.5), "BENCHMARKTOTALRETURN":(-1.5,1.5), "DURATIONEFFECT":(-0.01, 0.01), "INCOMEEFFECT": (-0.01, 0.01)}
        non_negative_fields = ["ACCOUNTAVERAGEWEIGHT", "BENCHMARKAVERAGEWEIGHT"]

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM FIXEDINCOMEATTRIBUTION WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = fi_attribution_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                primary_categories = temp_df['CATEGORY'].unique()
                category_schemes = temp_df['CATEGORYSCHEME'].unique()
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM FIXEDINCOMEATTRIBUTION WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        if 'Total' in primary_categories:
                            primary_categories = primary_categories[primary_categories != 'Total']
                        # secondary_categories = temp_df['SECONDARYCATEGORY'].unique()
                        # tertiary_categories = temp_df['TERTIARYCATEGORY'].unique()
                        benchmarks = temp_df['BENCHMARKCODE'].unique()
                        periods = temp_df['PERIOD'].unique()
                        currency_codes = temp_df['CURRENCYCODE'].unique()
                        benchmark_total_return = 0
                        # Addressing Total category first
                        total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total') &
                                (temp_df["SECONDARYCATEGORY"].isna() | (temp_df["SECONDARYCATEGORY"] == 'NULL')) &
                                (temp_df["TERTIARYCATEGORY"].isna() | (temp_df["TERTIARYCATEGORY"] == 'NULL'))]
                        # print(total_category_df)
                        for category_scheme in category_schemes:
                            category_scheme_df = total_category_df[(total_category_df['CATEGORYSCHEME'] == category_scheme)]
                            for benchmark in benchmarks:
                                benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                for period in periods:
                                    period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                    for currency_code in currency_codes:
                                        currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                        currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                        currency_df['ACCOUNTAVERAGEWEIGHT'] = 100
                                        currency_df['BENCHMARKAVERAGEWEIGHT'] = 100
                                        result_df = pd.concat([result_df, currency_df])

                        #Addressing primary categories
                        for primary_category in primary_categories:
                            primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category)]
                            # primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category) &
                            #     (temp_df["SECONDARYCATEGORY"].isna() | (temp_df["SECONDARYCATEGORY"] == 'NULL')) &
                            #     (temp_df["TERTIARYCATEGORY"].isna() | (temp_df["TERTIARYCATEGORY"] == 'NULL'))]
                            for category_scheme in category_schemes:
                                category_scheme_df = primary_category_df[(primary_category_df['CATEGORYSCHEME'] == category_scheme)]
                                for benchmark in benchmarks:
                                    benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                    for period in periods:
                                        period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                        for currency_code in currency_codes:
                                            currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                            currency_df = add_random_to_columns(currency_df, random_value_ranges, non_negative_fields)
                                            currency_df = adjust_sum_of_columns_no_negative(currency_df, 'ACCOUNTAVERAGEWEIGHT', 100)
                                            currency_df = adjust_sum_of_columns_no_negative(currency_df, 'BENCHMARKAVERAGEWEIGHT', 100)
                                            currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                            currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                            benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                            currency_df["ALLOCATIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                            currency_df["SELECTIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                            currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONEFFECT"] + currency_df["DURATIONEFFECT"] + currency_df["INCOMEEFFECT"]
                                            result_df = pd.concat([result_df, currency_df])

                else:
                    if 'Total' in primary_categories:
                        primary_categories = primary_categories[primary_categories != 'Total']
                    # secondary_categories = temp_df['SECONDARYCATEGORY'].unique()
                    # tertiary_categories = temp_df['TERTIARYCATEGORY'].unique()
                    benchmarks = temp_df['BENCHMARKCODE'].unique()
                    periods = temp_df['PERIOD'].unique()
                    currency_codes = temp_df['CURRENCYCODE'].unique()
                    benchmark_total_return = 0
                    # Addressing Total category first
                    total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total') &
                                                (temp_df["SECONDARYCATEGORY"].isna() | (
                                                            temp_df["SECONDARYCATEGORY"] == 'NULL')) &
                                                (temp_df["TERTIARYCATEGORY"].isna() | (
                                                            temp_df["TERTIARYCATEGORY"] == 'NULL'))]
                    # print(total_category_df)
                    for category_scheme in category_schemes:
                        category_scheme_df = total_category_df[(total_category_df['CATEGORYSCHEME'] == category_scheme)]
                        for benchmark in benchmarks:
                            benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                            for period in periods:
                                period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                for currency_code in currency_codes:
                                    currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                    currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                        non_negative_fields)
                                    currency_df['ACCOUNTAVERAGEWEIGHT'] = 100
                                    currency_df['BENCHMARKAVERAGEWEIGHT'] = 100
                                    result_df = pd.concat([result_df, currency_df])

                    # Addressing primary categories
                    for primary_category in primary_categories:
                        primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category)]
                        # primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category) &
                        #     (temp_df["SECONDARYCATEGORY"].isna() | (temp_df["SECONDARYCATEGORY"] == 'NULL')) &
                        #     (temp_df["TERTIARYCATEGORY"].isna() | (temp_df["TERTIARYCATEGORY"] == 'NULL'))]
                        for category_scheme in category_schemes:
                            category_scheme_df = primary_category_df[
                                (primary_category_df['CATEGORYSCHEME'] == category_scheme)]
                            for benchmark in benchmarks:
                                benchmark_df = category_scheme_df[(category_scheme_df['BENCHMARKCODE'] == benchmark)]
                                for period in periods:
                                    period_df = benchmark_df[(benchmark_df['PERIOD'] == period)]
                                    for currency_code in currency_codes:
                                        currency_df = period_df[(period_df['CURRENCYCODE'] == currency_code)]
                                        currency_df = add_random_to_columns(currency_df, random_value_ranges,
                                                                            non_negative_fields)
                                        currency_df = adjust_sum_of_columns_no_negative(currency_df,
                                                                                        'ACCOUNTAVERAGEWEIGHT', 100)
                                        currency_df = adjust_sum_of_columns_no_negative(currency_df,
                                                                                        'BENCHMARKAVERAGEWEIGHT', 100)
                                        currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - \
                                                                                currency_df["BENCHMARKAVERAGEWEIGHT"]
                                        currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - \
                                                                              currency_df["BENCHMARKTOTALRETURN"]
                                        benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark) & (
                                                    result_df['PERIOD'] == period) & (result_df[
                                                                                          'CURRENCYCODE'] == currency_code)][
                                            'BENCHMARKTOTALRETURN'].iloc[0]
                                        currency_df["ALLOCATIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (
                                                    currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                        currency_df["SELECTIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (
                                                    currency_df["ACCOUNTTOTALRETURN"] - currency_df[
                                                "BENCHMARKTOTALRETURN"])
                                        currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df[
                                            "SELECTIONEFFECT"] + currency_df["DURATIONEFFECT"] + currency_df[
                                                                         "INCOMEEFFECT"]
                                        result_df = pd.concat([result_df, currency_df])

            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                fi_attribution_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from FIXEDINCOMEATTRIBUTION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                fi_attribution_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in
                                                                                                           sample_data_info.description])

        return result_df


    def create_fixed_income_portfolio_characteristics_data(self):
        general_info_df = self.portfolio_general_info_df
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM PORTFOLIOCHARACTERISTICS WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' AND HISTORYDATE = '{base_as_of_date}'"
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            if result > 0:
                break
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select * from PORTFOLIOCHARACTERISTICS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
        port_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                            columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"CHARACTERISTICVALUE": (-0.5, 0.5)}
        non_negative_fields = ["CHARACTERISTICVALUE"]

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM PORTFOLIOCHARACTERISTICS WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                temp_df = port_characteristics_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM PORTFOLIOCHARACTERISTICS WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        for temp_row_index, temp_row in temp_df.iterrows():
                            if temp_row['CHARACTERISTICDISPLAYNAME'] == 'Fitch':
                                temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A+', 'A', 'A-'])
                            elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'Mdys':
                                temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A1', 'A2', 'A3'])
                            elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'Average Quality':
                                temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A1', 'Aa2', 'BAA2', 'Baa1'])
                            elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'S&P':
                                temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A', 'BAA+', 'BBB+'])
                            else:
                                temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    for temp_row_index, temp_row in temp_df.iterrows():
                        if temp_row['CHARACTERISTICDISPLAYNAME'] == 'Fitch':
                            temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A+', 'A', 'A-'])
                        elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'Mdys':
                            temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A1', 'A2', 'A3'])
                        elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'Average Quality':
                            temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(
                                ['A1', 'Aa2', 'BAA2', 'Baa1'])
                        elif temp_row['CHARACTERISTICDISPLAYNAME'] == 'S&P':
                            temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A', 'BAA+', 'BBB+'])
                        else:
                            temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                port_characteristics_sample_data_df = result_df[(result_df['PORTFOLIOCODE'] == self.base_portfolio_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select * from PORTFOLIOCHARACTERISTICS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'")
                port_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                columns=[x[0] for x in
                                                                                         sample_data_info.description])
        return result_df


    def create_fixed_income_benchmark_characteristics_data(self):
        general_info = self.conn.cursor().execute(f"SELECT DISTINCT(b.BENCHMARKCODE) FROM PORTFOLIOBENCHMARKASSOCIATION b "
                                                  f"INNER JOIN PORTFOLIOATTRIBUTES p ON p.PORTFOLIOCODE = b.PORTFOLIOCODE "
                                                  f"WHERE p.ATTRIBUTETYPECODE = '{self.strategy_code}'")
        general_info_df = pd.DataFrame.from_records(iter(general_info),
                                                    columns=[x[0] for x in general_info.description])
        base_as_of_date = self.base_as_of_date
        while True:
            query_template = f"SELECT COUNT(*) FROM BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' AND HISTORYDATE = '{base_as_of_date}'"
            # Execute the query
            result = self.conn.cursor().execute(query_template).fetchone()[0]
            # Check if the result/count is greater than 0
            if result > 0:
                break
            # Update the base_as_of_date to the prior month end
            base_as_of_date = self.prior_month_end(base_as_of_date)
        sample_data_info = self.conn.cursor().execute(
            f"select DISTINCT * from BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
        bench_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                            columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"CHARACTERISTICVALUE": (-0.5, 0.5)}
        non_negative_fields = ["CHARACTERISTICVALUE"]

        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT BENCHMARKCODE, COUNT(*) FROM BENCHMARKCHARACTERISTICS WHERE HISTORYDATE = '{as_of_date}' GROUP BY BENCHMARKCODE").fetchall()
            for row_index, row in general_info_df.iterrows():
                benchmark_code = row['BENCHMARKCODE']
                temp_df = bench_characteristics_sample_data_df.copy(deep=True)
                temp_df['BENCHMARKCODE'] = benchmark_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == benchmark_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM BENCHMARKCHARACTERISTICS WHERE BENCHMARKCODE = '{benchmark_code}' and HISTORYDATE = '{as_of_date}'")
                        for temp_row_index, temp_row in temp_df.iterrows():
                            if temp_row['CHARACTERISTICDISPLAYNAME'] == 'Average Quality':
                                temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(['A1', 'Aa2', 'BAA2', 'Baa1'])
                            else:
                                temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    for temp_row_index, temp_row in temp_df.iterrows():
                        if temp_row['CHARACTERISTICDISPLAYNAME'] == 'Average Quality':
                            temp_df.at[temp_row_index, 'CHARACTERISTICVALUE'] = random.choice(
                                ['A1', 'Aa2', 'BAA2', 'Baa1'])
                        else:
                            temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
            try:
                bench_characteristics_sample_data_df = result_df[(result_df['BENCHMARKCODE'] == self.base_benchmark_code) & (
                        result_df['HISTORYDATE'] == base_as_of_date)]
            except:
                sample_data_info = self.conn.cursor().execute(
                    f"select DISTINCT * from BENCHMARKCHARACTERISTICS where BENCHMARKCODE = '{self.base_benchmark_code}' and HISTORYDATE = '{base_as_of_date}'")
                bench_characteristics_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info),
                                                                                 columns=[x[0] for x in
                                                                                          sample_data_info.description])


        return result_df


    def create_purchases_sales_summary(self):
        print("Generating Purchases and Sales Summary Data")
        general_info_df = self.portfolio_general_info_df
        portfolio_codes = general_info_df['PORTFOLIOCODE'].unique()
        np.random.shuffle(portfolio_codes)
        split_index = int(len(portfolio_codes) * 0.8)
        rep_account_code_list = portfolio_codes[:split_index].tolist()

        base_as_of_date = self.base_as_of_date
        # while True:
        #     query_template = f"SELECT COUNT(*) FROM PURCHASESANDSALESSUMMARY WHERE PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}'"
        #     result = self.conn.cursor().execute(query_template).fetchone()[0]
        #     if result > 0:
        #         break
        #     base_as_of_date = self.prior_month_end(base_as_of_date)
            
        sample_data_info = self.conn.cursor().execute(f"select ISSUENAME, HISTORYDATE, MARKETVALUE from HOLDINGSDETAILS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{base_as_of_date}' and ASSETCLASSNAME <> 'Cash and Equiv.'")  
        holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        
        holdings_sample_data_df["MARKETVALUE"] = holdings_sample_data_df["MARKETVALUE"].astype(float)

        purchases_df = holdings_sample_data_df.nlargest(10, 'MARKETVALUE')
        sales_df = holdings_sample_data_df.nsmallest(10, 'MARKETVALUE')
        
        column_rename ={"MARKETVALUE":"TRADEAMOUNT"}

        purchases_3mt_df = purchases_df.copy(deep=True)
        sales_3mt_df = sales_df.copy(deep=True)

        purchases_3mt_df = purchases_3mt_df.nlargest(5, 'MARKETVALUE')
        sales_3mt_df = sales_3mt_df.nsmallest(5, 'MARKETVALUE')

        purchases_3mt_df = purchases_3mt_df.rename(columns=column_rename)
        sales_3mt_df = sales_3mt_df.rename(columns=column_rename)

        purchases_3mt_df["TRADEAMOUNT"] = purchases_3mt_df["TRADEAMOUNT"] * 0.015
        sales_3mt_df["TRADEAMOUNT"] = sales_3mt_df["TRADEAMOUNT"] * 0.015

        purchases_3mt_df["PERIOD"] = "3MT"
        sales_3mt_df["PERIOD"] = "3MT"

        purchases_3mt_df["TRANSACTIONTYPE"] = "Buy"
        sales_3mt_df["TRANSACTIONTYPE"] = "Sell"

        purchases_3mt_df["TRANSACTIONCATEGORY"] = ""
        sales_3mt_df["TRANSACTIONCATEGORY"] = ""

        # Mark 3 random securities as "Added" and 2 as "Initiated" in purchases df
        purchases_3mt_df.loc[purchases_3mt_df.sample(n=3).index, "TRANSACTIONCATEGORY"] = "Added"
        purchases_3mt_df.loc[purchases_3mt_df["TRANSACTIONCATEGORY"] == "", "TRANSACTIONCATEGORY"] = "Initiated"

        # Mark 3 records as "Trimmed" and 2 as "Liquidated" in the sales df
        sales_3mt_df.loc[sales_3mt_df.sample(n=3).index, "TRANSACTIONCATEGORY"] = "Trimmed"
        sales_3mt_df.loc[sales_3mt_df["TRANSACTIONCATEGORY"] == "", "TRANSACTIONCATEGORY"] = "Liquidated"

        # Generating purchases and sales for 1MT period
        purchases_1mt_df = purchases_df.copy(deep=True)
        sales_1mt_df = sales_df.copy(deep=True)

        # Get the second 5 largest records from purchases_1mt_df based on MarketValue
        purchases_1mt_df = purchases_1mt_df.nlargest(10, 'MARKETVALUE').iloc[5:]
        # Get the second 5 smallest records from sales_1mt_df based on MarketValue  
        sales_1mt_df = sales_1mt_df.nsmallest(10, 'MARKETVALUE').iloc[5:]

        purchases_1mt_df = purchases_1mt_df.rename(columns=column_rename)
        sales_1mt_df = sales_1mt_df.rename(columns=column_rename)

        purchases_1mt_df["TRADEAMOUNT"] = purchases_1mt_df["TRADEAMOUNT"] * 0.01
        sales_1mt_df["TRADEAMOUNT"] = sales_1mt_df["TRADEAMOUNT"] * 0.01

        purchases_1mt_df["PERIOD"] = "1MT"
        sales_1mt_df["PERIOD"] = "1MT"

        purchases_1mt_df["TRANSACTIONTYPE"] = "Buy"
        sales_1mt_df["TRANSACTIONTYPE"] = "Sell"

        purchases_1mt_df["TRANSACTIONCATEGORY"] = ""
        sales_1mt_df["TRANSACTIONCATEGORY"] = ""

        # Mark 3 random securities as "Added" and 2 as "Initiated" in purchases df
        purchases_1mt_df.loc[purchases_1mt_df.sample(n=3).index, "TRANSACTIONCATEGORY"] = "Added"
        purchases_1mt_df.loc[purchases_1mt_df["TRANSACTIONCATEGORY"] == "", "TRANSACTIONCATEGORY"] = "Initiated"

        # Mark 3 records as "Trimmed" and 2 as "Liquidated" in the sales df
        sales_1mt_df.loc[sales_1mt_df.sample(n=2).index, "TRANSACTIONCATEGORY"] = "Trimmed"
        sales_1mt_df.loc[sales_1mt_df["TRANSACTIONCATEGORY"] == "", "TRANSACTIONCATEGORY"] = "Liquidated"

        purchases_sales_sample_data_df = pd.concat([purchases_3mt_df, sales_3mt_df, purchases_1mt_df, sales_1mt_df])

        random_value_ranges_rep_accounts = { "TRADEAMOUNT":(-250,250)}
        random_value_ranges_non_rep_accounts = {"TRADEAMOUNT":(-1000,1000)}
        non_negative_fields = ["TRADEAMOUNT"]
        result_df = pd.DataFrame()
        time_list = self.time_periods_flexible()
        as_of_date, next_as_of_date = self.grab_needed_dates(time_list, base_as_of_date)

        while as_of_date <= self.as_of_date:
            count_by_group = self.conn.cursor().execute(
                f"SELECT PORTFOLIOCODE, COUNT(*) FROM PURCHASESANDSALESSUMMARY WHERE HISTORYDATE = '{as_of_date}' GROUP BY PORTFOLIOCODE").fetchall()
            
            for row_index, row in general_info_df.iterrows():
                portfolio_code = row['PORTFOLIOCODE']
                random_value_ranges = dict()
                if (portfolio_code in rep_account_code_list) or (portfolio_code == self.base_portfolio_code):
                    random_value_ranges = random_value_ranges_rep_accounts
                else:
                    random_value_ranges = random_value_ranges_non_rep_accounts

                print("Generating purchases and sales data for: ", portfolio_code)
                temp_df = purchases_sales_sample_data_df.copy(deep=True)
                temp_df['PORTFOLIOCODE'] = portfolio_code
                temp_df['HISTORYDATE'] = as_of_date
                if any(item[0] == portfolio_code for item in count_by_group):
                    if self.delete_and_insert:
                        self.conn.cursor().execute(
                            f"DELETE FROM PURCHASESANDSALESSUMMARY WHERE PORTFOLIOCODE = '{portfolio_code}' and HISTORYDATE = '{as_of_date}'")
                        temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                        result_df = pd.concat([result_df, temp_df])
                else:
                    temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
                    result_df = pd.concat([result_df, temp_df])
            
            if as_of_date == self.as_of_date:
                break
            base_as_of_date, as_of_date = self.grab_needed_dates(time_list, base_as_of_date)
        
        result_df = result_df.reset_index(drop=True)
        result_df = result_df[['PORTFOLIOCODE', 'HISTORYDATE', 'PERIOD','ISSUENAME', 'TRANSACTIONCATEGORY',  'TRANSACTIONTYPE', 'TRADEAMOUNT']]
        return result_df

    # # fixed income & equity
    def time_periods_flexible(self):
        # self.base_as_of_date vs self.as_of_date
        # as of date always closer today compare with base of date
        # Define the start date and the current date
        start_date = '2023-11-30'
        # the start_date_index should be 0
        end_date = datetime.now().strftime('%Y-%m-%d')

        # Generate a date range with end of month frequency
        date_range = pd.date_range(start=start_date, end=end_date, freq='ME')

        # Convert to list of strings
        end_of_month_dates = date_range.strftime('%Y-%m-%d').tolist()
        # print(end_of_month_dates)
        return end_of_month_dates

    def grab_needed_dates(self, end_of_month_dates, current_base_as_of_date):
        # Get the index for the current base_as_of_date
        index_base = end_of_month_dates.index(current_base_as_of_date)

        # Original as_of_date should be the date next to the base_as_of_date/start_date
        current_as_of_date = end_of_month_dates[index_base + 1]

        # But we want new dates
        new_base_of_date = current_as_of_date

        try:
            new_as_of_date = end_of_month_dates[index_base + 2]
        except:
            def get_end_of_month():
                today_date = datetime.now()
                # Get the year and month from the input date
                year = today_date.year
                month = today_date.month
                # Calculate the last day of the month
                if month == 12:
                    end_of_month = date(year, month, 31)
                else:
                    next_month = date(year, month + 1, 1)
                    end_of_month = next_month - timedelta(days=1)

                # Convert the end of month date back to string in 'year-month-date' format
                return end_of_month.strftime('%Y-%m-%d')

            # Get the end of the month for today's date
            new_as_of_date = get_end_of_month()

        return new_base_of_date, new_as_of_date

    def prior_month_end(self, provide_date):
        # Convert the input string to a datetime object
        date = datetime.strptime(provide_date, '%Y-%m-%d')

        # Find the first day of the current month
        first_day_of_current_month = date.replace(day=1)

        # Subtract one day to get the last day of the previous month
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

        return last_day_of_previous_month.strftime('%Y-%m-%d')


    # Function to randomly assign broker name and code
    def assign_broker(self, row):
        # Create a dictionary of broker names and their corresponding codes
        broker_dict = {
            "Instinet": "inm",
            "Luminex": "lum",
            "Liquidnet": "LQNT",
            "Cowen": "ljc",
            "Wolfe Trahan": "wt",
            "Piper Sandler & Co.": "wes",
            "BlockCross": "blc",
            "C.I.S. Sec.": "cis",
            "Virtu Financial": "dcb",
            "JP Morgan": "jsd",
            "JP Morgan (BIDS)": "JPBIDS",
            "UBS (BIDS Sponsor)": "ubbids",
            "Morgan Stanley": "mo",
            "UBS Warburg": "ubs",
            "Keefe Bruyette": "kb",
            "Cantor Fitzgerald": "cf",
            "Dowling & Partners Sec. LLC": "dow",
            "Strategas (BIDS)": "stbids",
            "Barclays": "br",
            "Citigroup": "cit"
        }

        # Convert the dictionary to a DataFrame
        broker_df = pd.DataFrame(list(broker_dict.items()), columns=['BROKERNAME', 'BROKERCODE'])
        # Store them
        random_broker = broker_df.sample(n=1).iloc[0]
        row['BROKERNAME'] = random_broker['BROKERNAME']
        row['BROKERCODE'] = random_broker['BROKERCODE']
        return row
