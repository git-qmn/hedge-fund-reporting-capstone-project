import pandas as pd
import numpy as np
import decimal
from decimal import Decimal
from DeriveValidPeriods import exec_script as ext
from DeriveValidPeriods import read_data as rdt
import string
import random
from open_ai_interactions import get_openai_client_obj, interact_with_chat_application, interact_with_gpt4

def add_random_float(x, min=-1, max = 1):
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
    
    def add_random_float_to_column(x, is_non_negative, min_val, max_val):
        random_addition = np.random.uniform(min_val, max_val)
        result = x + random_addition
        if is_non_negative and result < 0:
            return x + np.random.uniform(0, max_val)
        else:
            return result

    
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
        reassign_value = misaligned_sum / non_zero_rows

        for row_index, row in df.iterrows():
            if row[column_name] != 0:
                df.at[row_index, column_name] = row[column_name] - reassign_value
    
    return df

    
        


class QuantData:
    def __init__(self, conn, as_of_date, params, performance_period_mapping):
        self.conn = conn
        self.as_of_date = as_of_date
        self.params = params
        self.base_portfolio_code = self.params['base_portfolio_code']
        self.base_as_of_date = self.params['base_as_of_date']
        self.performance_period_mapping = performance_period_mapping
        self.portfolio_general_info = self.conn.cursor().execute("SELECT PORTFOLIOCODE, NAME, OPENDATE, PERFORMANCEINCEPTIONDATE, TERMINATIONDATE, BASECURRENCYCODE, BASECURRENCYNAME, PRODUCTCODE FROM portfoliogeneralinformation where portfoliocode = '2900'")
        self.portfolio_general_info_df = pd.DataFrame.from_records(iter(self.portfolio_general_info), columns=[x[0] for x in self.portfolio_general_info.description])

        self.openai_client = get_openai_client_obj()

    def generate_accounts(self):
        account_code_list = generate_account_codes(100)
        account_df = pd.DataFrame(columns=['PORTFOLIOCODE','NAME', 'INVESTMENTSTYLE', 'PORTFOLIOCATEGORY','OPENDATE','PERFORMANCEINCEPTIONDATE','TERMINATIONDATE','BASECURRENCYCODE','BASECURRENCYNAME','ISBEGINOFDAYPERFORMANCE','PRODUCTCODE'])
        account_name_request = f"You are gnerating syntheic data for an asset management firm. Generate 100 comma separated names appropriate for separately managed accounts managed under the strategy name {self.params['strategy_name']} and do not include numbering. These should be names of public companies, pension funds, non-profit organizations and private trusts."
        message_text = [{"role": "system", "content": "you are a synthetic data generator for an asset management firm. do not include any text other than the required answer and act as a completion service"},{"role": "user", "content": account_name_request}]
        content = interact_with_gpt4(message_text, self.openai_client)['choices'][0]['message']['content']
        account_names_list = content.split(',')
        strategy_inceeption_date = self.params['strategy_inception_date']
        inception_date_list = generate_random_dates(strategy_inceeption_date, '2023-04-30', 100)

        for account_code, account_name, inception_date in zip(account_code_list, account_names_list, inception_date_list):
            account_df = account_df.append({'PORTFOLIOCODE': account_code, 'NAME': account_name, 'INVESTMENTSTYLE': 'Growth', 'PORTFOLIOCATEGORY': 'Individual Account', 'OPENDATE': inception_date, 'PERFORMANCEINCEPTIONDATE': inception_date, 'TERMINATIONDATE': None, 'BASECURRENCYCODE': 'USD', 'BASECURRENCYNAME': 'US Dollar', 'ISBEGINOFDAYPERFORMANCE': False, 'PRODUCTOCDE': None}, ignore_index=True)
        account_df.to_csv('account_data.csv', index=False, sep='|')

        # for account_code in account_code_list:
            
                        
    def execute_data_generation(self):
        
        if self.params['portfolio_pre_calculated_performance_data']:
            pre_calc_port_return_df = self.create_portfolio_pre_calculated_performance_data()
            pre_calc_port_return_df.to_csv('portfolio_pre_calculated_performance_data.csv', index=False, sep='|')
        if self.params['benchmark_pre_calculated_performance_data']:
            self.create_benchmark_pre_calculated_performance_data()
        if self.params['account_benchmark_pre_calculated_performance_data']:
            self.create_account_benchmark_pre_calculated_performance_data()
        if self.params['holdings_data']:
            self.create_holdings_data()
        if self.params['attribution_data']:
            self.create_attribution_data()
        if self.params['sector_allocation_data']:
            self.create_sector_allocation_data()
        # if self.params['portfolio_characteristics_data']:
        #     self.create_portfolio_characteristics_data()
        # if self.params['benchmark_characteristics_data']:
        #     self.create_benchmark_characteristics_data()
        if self.params['portfolio_performance_factors']:
            result_df = self.create_performance_factors()
            result_df.to_csv('portfolio_performance_factors.csv', index=False, sep='|')
        if self.params['benchmark_prices']:
            result_df = self.create_benchmark_prices()
            result_df.to_csv('benchmark_performance_factors.csv', index=False, sep='|')
        self.conn.close()

    def create_portfolio_pre_calculated_performance_data(self):
        general_info_df = self.portfolio_general_info_df
        
        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedportfolioperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{self.base_as_of_date}' and PerformanceCategory = 'Asset Class' and PerformanceCategoryname = 'Total Portfolio'")
        port_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        required_period_list = list(self.performance_period_mapping.keys())
         
        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            # if row['PORTFOLIOCODE'] == 'ASTGVECOMP':
            valid_period_params = {'AsofDate': self.as_of_date, 'InceptionDate': str(row['PERFORMANCEINCEPTIONDATE']), 'FiscalYearEnd': '06-30', 'SuppressNotApplicablePeriods': 'yes', 'SuppressDuplicatePeriods': 'no', 'PeriodList': ','.join(required_period_list)}
            portfolio_code = row['PORTFOLIOCODE']
            temp_df = port_performance_sample_data_df.copy(deep=True)
            temp_df['PORTFOLIOCODE'] = portfolio_code
            temp_df['HISTORYDATE'] = self.as_of_date
            temp_df['PERFORMANCEINCEPTIONDATE'] = row['PERFORMANCEINCEPTIONDATE']
            temp_df['PORTFOLIOINCEPTIONDATE'] = row['OPENDATE']
            temp_df = temp_df.applymap(add_random_float)
            valid_period_response = ext(rdt, valid_period_params)
            valid_periods_df = pd.DataFrame.from_records(valid_period_response['data'])
            period_list = valid_periods_df['period'].unique().tolist()
            for period_key in self.performance_period_mapping.keys():
                if period_key not in period_list:
                    temp_df[self.performance_period_mapping[period_key]] = np.nan
            result_df = pd.concat([result_df, temp_df])
        return result_df

    def create_benchmark_pre_calculated_performance_data(self):
        general_info = self.conn.cursor().execute("SELECT BenchmarkCode FROM benchmarkgeneralinformation")
        general_info_df = pd.DataFrame.from_records(iter(general_info), columns=[x[0] for x in general_info.description])

        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedbenchmarkperformance where BENCHMARKCODE = 'sptotal' and HISTORYDATE = '{self.as_of_date}'")
        benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        
        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            benchmark_code = row['BENCHMARKCODE']
            temp_df = benchmark_performance_sample_data_df.copy(deep=True)
            temp_df['BENCHMARKCODE'] = benchmark_code
            temp_df['HISTORYDATE'] = self.as_of_date
            temp_df = temp_df.applymap(add_random_float)
            result_df = pd.concat([result_df, temp_df])
        return result_df
    
    def create_account_benchmark_pre_calculated_performance_data(self):
        general_info = self.conn.cursor().execute("SELECT PORTFOLIOCODE, BENCHMARKCODE  FROM portfoliobenchmarkassociation")
        general_info_df = pd.DataFrame.from_records(iter(general_info), columns=[x[0] for x in general_info.description])
        sample_data_info = self.conn.cursor().execute(f"select * from precalculatedaccountbenchmarkperformance where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{self.base_as_of_date}0' and BENCHMARKCODE = 'sptotal'")
        benchmark_performance_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        
        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            portfolio_code = row['PORTFOLIOCODE']
            benchmark_code = row['BENCHMARKCODE']
            temp_df = benchmark_performance_sample_data_df.copy(deep=True)
            temp_df['PORTFOLIOCODE'] = portfolio_code
            temp_df['BENCHMARKCODE'] = benchmark_code
            temp_df['HISTORYDATE'] = self.as_of_date
            temp_df = temp_df.applymap(add_random_float)
            result_df = pd.concat([result_df, temp_df])
        return result_df

    def create_holdings_data(self):
        
        general_info_df = self.portfolio_general_info_df
        sample_data_info = self.conn.cursor().execute(f"select * from HOLDINGSDETAILS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{self.base_as_of_date}'")
        holdings_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = {"QUANTITY":(-100,100), "MARKETVALUEWITHOUTACCRUEDINCOME":(-10000,10000), "LOCALMARKETVALUE": (-10000,10000), "UNREALIZEDGAINSLOSSES":(-1000,1000), "ACCRUEDINCOME":(-100,100), "ESTIMATEDANNUALINCOME":(-250,250), "PRICE": (-10,10) }
        non_negative_fields = ["QUANTITY", "MARKETVALUEWITHOUTACCRUEDINCOME", "LOCALMARKETVALUE", "ESTIMATEDANNUALINCOME", "PRICE"]

        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            # if row['PORTFOLIOCODE'] == 'ASTGVECOMP':
            
            portfolio_code = row['PORTFOLIOCODE']
            temp_df = holdings_sample_data_df.copy(deep=True)
            temp_df['PORTFOLIOCODE'] = portfolio_code
            temp_df['HISTORYDATE'] = self.as_of_date
            temp_df = add_random_to_columns(temp_df, random_value_ranges, non_negative_fields)
            result_df = pd.concat([result_df, temp_df])
        return result_df

    def create_attribution_data(self):
        general_info_df = self.portfolio_general_info_df
        sample_data_info = self.conn.cursor().execute(f"select * from ATTRIBUTION where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{self.base_as_of_date}'")
        attribution_sample_data_df = pd.DataFrame.from_records(iter(sample_data_info), columns=[x[0] for x in sample_data_info.description])
        random_value_ranges = { "ACCOUNTAVERAGEWEIGHT":(-5,5), "BENCHMARKAVERAGEWEIGHT": (-5,5), "ACCOUNTTOTALRETURN":(-2.5,2.5), "BENCHMARKTOTALRETURN":(-1.5,1.5)}
        non_negative_fields = ["ACCOUNTAVERAGEWEIGHT", "BENCHMARKAVERAGEWEIGHT"]

        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            # if row['PORTFOLIOCODE'] == 'ASTGVECOMP':
            
            portfolio_code = row['PORTFOLIOCODE']
            temp_df = attribution_sample_data_df.copy(deep=True)
            temp_df['PORTFOLIOCODE'] = portfolio_code
            temp_df['HISTORYDATE'] = self.as_of_date
            primary_categories = temp_df['CATEGORY'].unique()
            category_schemes = temp_df['CATEGORYSCHEME'].unique()

            if 'Total' in primary_categories:
                primary_categories = primary_categories[primary_categories != 'Total']
            secondary_categories = temp_df['SECONDARYCATEGORY'].unique()
            tertirary_categories = temp_df['TERTIARYCATEGORY'].unique()
            benchmarks = temp_df['BENCHMARKCODE'].unique()
            periods = temp_df['PERIOD'].unique()
            currency_codes = temp_df['CURRENCYCODE'].unique() 
            benchmark_total_return = 0

            # Addressing Total category first
            total_category_df = temp_df[(temp_df['CATEGORY'] == 'Total')&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]  
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

            #Addressing primary categories

            for primary_category in primary_categories:
                primary_category_df = temp_df[(temp_df['CATEGORY'] == primary_category)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
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
                                benchmark_total_return = result_df[(result_df['BENCHMARKCODE'] == benchmark)&(result_df['PERIOD'] == period)&(result_df['CURRENCYCODE'] == currency_code)]['BENCHMARKTOTALRETURN'].iloc[0]
                                currency_df["ALLOCATIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                currency_df["SELECTIONANDINTERACTIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                result_df = pd.concat([result_df, currency_df])
            
            #Addressing secondary categories

            for secondary_category in secondary_categories:
                secondary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == secondary_category)&(temp_df["TERTIARYCATEGORY"] == np.nan)]
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
                                currency_df["ALLOCATIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                currency_df["SELECTIONANDINTERACTIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                result_df = pd.concat([result_df, currency_df])


            #Addressing tertiary categories
            for tertiary_category in tertirary_categories:
                tertiary_category_df = temp_df[(temp_df['CATEGORY'] == np.nan)&(temp_df["SECONDARYCATEGORY"] == np.nan)&(temp_df["TERTIARYCATEGORY"] == tertiary_category)]
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
                                currency_df["ALLOCATIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["BENCHMARKTOTALRETURN"] - benchmark_total_return)
                                currency_df["SELECTIONANDINTERACTIONEFFECT"] = currency_df["BENCHMARKAVERAGEWEIGHT"] * (currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"] )
                                currency_df["TOTALEFFECT"] = currency_df["ALLOCATIONEFFECT"] + currency_df["SELECTIONANDINTERACTIONEFFECT"]
                                currency_df["VARIATIONAVERAGEWEIGHT"] = currency_df["ACCOUNTAVERAGEWEIGHT"] - currency_df["BENCHMARKAVERAGEWEIGHT"]
                                currency_df["VARIATIONTOTALRETURN"] = currency_df["ACCOUNTTOTALRETURN"] - currency_df["BENCHMARKTOTALRETURN"]
                                result_df = pd.concat([result_df, currency_df])

        return result_df
        

    def create_sector_allocation_data(self):
        #NOTE : The sector allocation data will depend on the holdings data. Make sure to generate the synthetic holdings data before generating the sector allocation data
        
        general_info_df = self.portfolio_general_info_df

        def calculate_sector_allocation(holdings_df):
            primary_sector_scheme = holdings_df['PRIMARYSECTORSCHEME'].iloc[0]
            history_date = holdings_df['HISTORYDATE'].iloc[0]
            currency_code = 'USD'
            portfolio_code = holdings_df['PORTFOLIOCODE'].iloc[0]
            currency = 'US Dollar'
            language_code = 'en-US'
            category = 'Sector'
            sector_allocation = holdings_df.groupby('PRIMARYSECTORNAME')['MARKETVALUEWITHOUTACCRUEDINCOME'].sum()
            sector_allocation['PORTFOLIOCODE'] = portfolio_code
            sector_allocation['HISTORYDATE'] = history_date
            sector_allocation['SECTORSCHEME'] = primary_sector_scheme
            sector_allocation['CURRENCYCODE'] = currency_code
            sector_allocation['CURRENCY'] = currency
            sector_allocation['LANGUAGECODE'] = language_code
            sector_allocation['CATEGORY'] = category
            sector_allocation['MARKETVALUE'] = sector_allocation['MARKETVALUEWITHOUTACCRUEDINCOME']
            sector_allocation = sector_allocation.rename({'PRIMARYSECTORNAME': 'CATEGORYNAME'}, axis=1)
            sector_allocation['PORTFOLIOWEIGHT'] = np.nan
 
            sector_allocation['PORTFOLIOWEIGHT'] = 100 * sector_allocation['MARKETVALUEWITHOUTACCRUEDINCOME'] / sector_allocation['MARKETVALUEWITHOUTACCRUEDINCOME'].sum()
            sector_allocation['INDEX1WEIGHT'] = np.nan
            sector_allocation['INDEX2WEIGHT'] = np.nan
            sector_allocation['INDEX3WEIGHT'] = np.nan

            for index, row in sector_allocation.iterrows():
                sector_allocation.at[index, 'INDEX1WEIGHT'] = sector_allocation.at[index, 'PORTFOLIOWEIGHT'] + np.random.uniform(-1,1)

            sector_allocation = sector_allocation[['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'LANGUAGECODE', 'MARKETVALUEWITHOUTACCRUEDINCOME', 'MARKETVALUE', 'SECTORSCHEME', 'CATEGORY', 'CATEGORYNAME',  'PORTFOLIOWEIGHT']]
            return sector_allocation


        result_df = pd.DataFrame()
        for row_index, row in general_info_df.iterrows():
            # if row['PORTFOLIOCODE'] == 'ASTGVECOMP':
            holdings_data_df = pd.DataFrame()
            sector_allocation_df = pd.DataFrame()
            portfolio_holdings = self.conn.cursor().execute(f"select PRIMARYSECTORSCHEME, PRIMARYSECTORNAME, MARKETVALUEWITHOUTACCRUEDINCOME, MARKETVALUE from  HOLDINGSDETAILS where PORTFOLIOCODE = '{self.base_portfolio_code}' and HISTORYDATE = '{self.as_of_date}'")
            holdings_data_df = pd.DataFrame.from_records(iter(portfolio_holdings), columns=[x[0] for x in portfolio_holdings.description])
            sector_allocation_df = calculate_sector_allocation(holdings_data_df)
            sector_allocation_df = adjust_sum_of_column(sector_allocation_df, 'INDEX1WEIGHT', 1)
            result_df = pd.concat([result_df, sector_allocation_df])
        
        return result_df

    def create_performance_factors(self):
        general_info_df = self.portfolio_general_info_df
        performance_factor_columns =  ['PORTFOLIOCODE', 'HISTORYDATE', 'CURRENCYCODE', 'CURRENCY', 'PERFORMANCECATEGORY', 'PERFORMANCECATEGORYNAME', 'PERFORMANCETYPE', 'PERFORMANCEINCEPTIONDATE', 'PORTFOLIOINCEPTIONDATE', 'PERFORMANCEFREQUENCY','PERFORMANCEFACTOR']     
        result_df = pd.DataFrame(columns=performance_factor_columns)

        for row_index, row in general_info_df.iterrows():
            temp_df = pd.DataFrame(columns=performance_factor_columns)
            performance_inception_date = row['PERFORMANCEINCEPTIONDATE']
            last_performance_factor_date_query = f"SELECT max(HISTORYDATE) FROM portfolioperformance WHERE PORTFOLIOCODE = '{row['PORTFOLIOCODE']}'"
            last_performance_factor_date = self.conn.cursor().execute(last_performance_factor_date_query).fetchone()[0]
            if last_performance_factor_date is None:
                last_performance_factor_date = performance_inception_date
            else:
                last_performance_factor_date = (pd.to_datetime(last_performance_factor_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S.%f')

            date_range = pd.date_range(start=last_performance_factor_date, end=self.as_of_date, freq='D')
            temp_df['HISTORYDATE'] = date_range
            temp_df['PORTFOLIOCODE'] = row['PORTFOLIOCODE']
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
        return result_df

    def create_benchmark_prices(self):
        performance_factor_columns =  ['BENCHMARKCODE','PERFORMANCEDATATYPE', 'CURRENCYCODE', 'CURRENCY',  'PERFORMANCEFREQUENCY','VALUE','HISTORYDATE']     
        result_df = pd.DataFrame(columns=performance_factor_columns)

        unique_benchmark_query_result= self.conn.cursor().execute(f"SELECT distinct BENCHMARKCODE FROM PORTFOLIOBENCHMARKASSOCIATION")
        unique_benchmark_info_df = pd.DataFrame.from_records(iter(unique_benchmark_query_result), columns=[x[0] for x in unique_benchmark_query_result.description])

        for row_index, row in unique_benchmark_info_df.iterrows():
            temp_df = pd.DataFrame(columns=performance_factor_columns)
            default_incpeption_date = '2000-01-01'

            last_price_date_query = f"SELECT max(HISTORYDATE) FROM benchmarkperformance WHERE BENCHMARKCODE = '{row['BENCHMARKCODE']}'"
            last_price_date = self.conn.cursor().execute(last_price_date_query).fetchone()[0]
            
            last_price = 100

            if last_price_date is None:
                last_price_date = default_incpeption_date
                last_price = 100
            else:
                last_price = self.conn.cursor().execute(f"SELECT VALUE FROM benchmarkperformance WHERE BENCHMARKCODE = '{row['BENCHMARKCODE']}' AND HISTORYDATE = '{last_price_date}'").fetchone()[0]
                last_price_date = (pd.to_datetime(last_price_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
            date_range = pd.date_range(start=last_price_date, end=self.as_of_date, freq='D')
            temp_df['HISTORYDATE'] = date_range
            temp_df['BENCHMARKCODE'] = row['BENCHMARKCODE']
            temp_df['CURRENCYCODE'] = 'USD'
            temp_df['CURRENCY'] = 'US Dollar'
            temp_df['PERFORMANCEDATATYPE'] = 'Prices'
            temp_df['PERFORMANCEFREQUENCY'] = np.nan
            temp_df['VALUE'] = np.nan
            for i in range(len(temp_df)):
                if i == 0:
                    temp_df.at[i, 'VALUE'] = last_price * (1 + np.random.normal(0.001, 0.001))
                else:
                    temp_df.at[i, 'VALUE'] = temp_df.at[i-1, 'VALUE'] * (1 + np.random.normal(0.001, 0.001))
            result_df = pd.concat([result_df, temp_df])

        return result_df