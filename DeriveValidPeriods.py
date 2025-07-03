import pandas as pd
import datetime
from fn_GetBeginEndDates import exec_script as ext
from fn_GetBeginEndDates import read_data as rdt


def read_data(name, parameters):
    if name == "ast_fn_GetBeginEndDates":
        reply = {}
        ext(reply, rdt, parameters)
        return reply


def exec_script(read, params):
    response = {}
    suppress_not_applicable = params["SuppressNotApplicablePeriods"]
    suppress_duplicate_periods = params["SuppressDuplicatePeriods"]
    period_list = params["PeriodList"].split(",")
    period_func_params = {"AsofDate": params["AsofDate"], "PeriodCode": "", "InceptionDate": params["InceptionDate"], "FiscalYearEnd": params["FiscalYearEnd"]}
    inception_date = datetime.datetime.strptime(params["InceptionDate"],'%Y-%m-%d')
    period_frame = pd.DataFrame(columns=["period", "begin_date", "end_date", "is_annualized"])
    period_index = 0
    for period_code in period_list:
        period_func_params["PeriodCode"] = period_code
        period_begin_end_dates = read("ast_fn_GetBeginEndDates", period_func_params)["data"]
        begin_date = datetime.datetime.strptime(period_begin_end_dates[0]["begin_date"], '%Y-%m-%d')
        end_date = datetime.datetime.strptime(period_begin_end_dates[0]["end_date"], '%Y-%m-%d')
        period_frame.at[period_index, "period"] = period_code
        period_frame.at[period_index, "begin_date"] = begin_date
        period_frame.at[period_index, "end_date"] = end_date
        if period_code.endswith("YA") or period_code == "ITDA":
            period_frame.at[period_index, "is_annualized"] = True
        else:
            period_frame.at[period_index, "is_annualized"] = False
        period_index += 1

    if suppress_not_applicable == "yes":
        period_frame = period_frame.loc[period_frame['begin_date'] >= inception_date]
    
    if suppress_duplicate_periods == "yes":
        period_frame = period_frame.drop_duplicates(["begin_date","end_date", "is_annualized"])

    period_frame['begin_date'] = period_frame['begin_date'].astype(str)
    period_frame['end_date'] = period_frame['end_date'].astype(str)

    output_list = period_frame.to_dict(orient='records')
    response['data'] = output_list
    return response


def test_get_valid_periods():
    # parameters = {"AsofDate": "2020-02-29", "InceptionDate": "2010-05-21","FiscalYearEnd": "04-30",
    #               "SuppressNotApplicablePeriods": "yes", "SuppressDuplicatePeriods": "yes",
    #               "PeriodList":"MTD,QTD,YTD,3MT,12MT,3YA,5YA,10YA,15YA,30YA,FYTD,ITD,ITDA"}
    parameters = {'AsofDate': '2023-04-30', 'InceptionDate': '2000-10-31', 'FiscalYearEnd': '06-30', 'SuppressNotApplicablePeriods': 'yes', 'SuppressDuplicatePeriods': 'yes', 'PeriodList': 'QTD,YTD,PQ1,PQ2,PQ3,PQ4,PY1,PY2,PY3,PY4,PY5,PY6,PY7,PY8,PY9,PY10,1MT,3MT,6MT,9MT,12MT,2YC,3YC,4YC,5YC,6YC,7YC,8YC,9YC,10YC,12YC,15YC,20YC,25YC,30YC,ITD,2YA,3YA,4YA,5YA,6YA,7YA,8YA,9YA,10YA,12YA,15YA,20YA,25YA,30YA,ITDA,FYTD,PFQ1,PFQ2,PFQ3,PFQ4,PFY1,PFY2,PFY3,PFY4,PFY5,PFY6,PFY7,PFY8,PFY9,PFY10'}
    response = {}
    exec_script(response, read_data, parameters)
    print(response)


# test_get_valid_periods()

# def period_universe_json():
#     file_name = 'period_universe.xlsx'
#     base_path = os.path.abspath(os.path.dirname(__file__))
#     file_path = os.path.join(base_path, file_name)
#
#     period_universe_df = pd.read_excel(file_path)
#     print(period_universe_df)
#
#     output_dict = dict()
#     for row_index, data_row in period_universe_df.iterrows():
#         temp_dict = dict()
#         temp_dict['DisplayName'] = data_row['DisplayName']
#         temp_dict['Years'] = data_row['Years']
#         temp_dict['Months'] = data_row['Months']
#         temp_dict['Quarters'] = data_row['Quarters']
#         output_dict[data_row['Period']] = temp_dict
#
#     out_file = open("period_universe.json", "w")
#
#     json.dump(output_dict, out_file, indent=6)
#
#     out_file.close()
#
#
# period_universe_json()