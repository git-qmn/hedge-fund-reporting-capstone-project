import os
import json
import datetime
import calendar as calendar
import functools as ft


def read_data(name, parameters={}):
    file_name = 'period_universe.json'
    base_path = os.path.abspath(os.path.dirname(__file__))
    file_path = os.path.join(base_path, file_name)

    if name == "sys_period_information":
        with open(file_path) as input_file:
            json_data_file = json.load(input_file)

        arr = json_data_file
        return arr
    else:
        raise Exception("Unsupported block name")


def exec_script(response, read, params):
    """
    :param params:
    example parameters
    "AsofDate": "2021-05-31",
    "PeriodCode":"PFQ1",
    "FiscalYearEnd": "04-30",
    "InceptionDate": "2015-09-08"

    :return:
    {'data': [{'begin_date': '2021-07-31', 'end_date': '2020-10-31'}]}
    """
    period_universe = read("sys_period_information")
    # print(period_universe)
    date_pattern = ""
    period_code = params["PeriodCode"].strip()
    as_of_date = params["AsofDate"]
    if "DatePattern" in params.keys():
        date_pattern = params["DatePattern"].strip()

    if len(date_pattern) == 0:
        date_pattern = '%Y-%m-%d'

    period_begin_date = as_of_date
    period_end_date = as_of_date

    date_obj_begin = datetime.datetime.strptime(as_of_date, date_pattern)
    date_obj_end = datetime.datetime.strptime(as_of_date, date_pattern)

    if period_code == "MTD":
        month = date_obj_begin.month
        period_begin_year = date_obj_begin.year
        period_begin_month = month - 1
        if period_begin_month <= 0:
            period_begin_month = 12 + period_begin_month
            period_begin_year = date_obj_begin.year - 1

        res = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res)
        period_end_date = date_obj_end

    if period_code == "QTD":
        month = date_obj_begin.month
        year = date_obj_begin.year
        period_begin_month = month
        period_begin_year = year

        if month <= 3:
            period_begin_month = 12
            period_begin_year = year - 1
        elif 3 < month <= 6:
            period_begin_month = 3
        elif 6 < month <= 9:
            period_begin_month = 6
        elif 9 < month <= 12:
            period_begin_month = 9

        res = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res)
        period_end_date = date_obj_end

    if period_code == "YTD":
        year = date_obj_begin.year
        period_begin_month = 12
        period_begin_year = year - 1

        res = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res)
        period_end_date = date_obj_end

    if period_code == "MRQ":
        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year

        if (month <= 2 and day <= 31) or (month == 3 and day < 30):
            period_begin_month = 9
            period_end_month = 12
            period_begin_year = year - 1
            period_end_year = year - 1

        elif (3 <= month <= 5 and day <= 31) or (month == 6 and day < 30):
            period_begin_month = 12
            period_end_month = 3
            period_begin_year = year - 1

        elif (6 <= month <= 8 and day <= 31) or (month == 9 and day < 29):
            period_begin_month = 3
            period_end_month = 6

        elif (9 <= month <= 11 and day <= 30) or (month == 12 and day < 30):
            period_begin_month = 6
            period_end_month = 9

        elif month == 12 and day == 31:
            period_begin_month = 9
            period_end_month = 12

        res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        res_end = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)
        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res_begin)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month, day=res_end)

    if period_code.startswith("PQ"):
        period_details = period_universe[period_code]
        num_quarters = int(period_details['Quarters'])

        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year

        month_delta = num_quarters * 3

        # Calculate MRQ Begin and end dates
        if (month <= 2 and day <= 31) or (month == 3 and day < 30):
            period_begin_month = 9
            period_end_month = 12
            period_begin_year = year - 1
            period_end_year = year - 1

        elif (3 <= month <= 5 and day <= 31) or (month == 6 and day < 30):
            period_begin_month = 12
            period_end_month = 3
            period_begin_year = year - 1

        elif (6 <= month <= 8 and day <= 31) or (month == 9 and day < 29):
            period_begin_month = 3
            period_end_month = 6

        elif (9 <= month <= 11 and day <= 30) or (month == 12 and day < 30):
            period_begin_month = 6
            period_end_month = 9

        elif month == 12 and day == 31:
            period_begin_month = 9
            period_end_month = 12

        res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        res_end = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)
        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res_begin)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month, day=res_end)
        ####

        begin_year = period_begin_date.year
        begin_month = period_begin_date.month

        end_year = period_end_date.year
        end_month = period_end_date.month

        begin_month = begin_month - month_delta
        end_month = end_month - month_delta

        if begin_month <= 0:
            begin_month = 12 + begin_month
            begin_year = begin_year - 1

        if end_month <= 0:
            end_month = 12 + end_month
            end_year = end_year - 1

        begin_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(begin_year, begin_month)], 30)
        end_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(end_year, end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=begin_year, month=begin_month, day=begin_day)
        period_end_date = date_obj_end.replace(year=end_year, month=end_month, day=end_day)

    if period_code.startswith("PY"):
        period_details = period_universe[period_code]
        num_years = int(period_details['Years'])

        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_year = year
        period_begin_month = month

        period_end_year = year
        period_end_month = month
        period_end_day = day

        if period_end_month == 12 and period_end_day == 31:
            period_begin_year -= (num_years + 1)
            period_end_year -= num_years
        else:
            period_begin_year -= (num_years + 2)
            period_end_year -= (num_years + 1)
            period_begin_month = 12
            period_end_month = 12

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=31)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month, day=31)

    if period_code == "MRFQ":
        month = date_obj_begin.month
        year = date_obj_begin.year

        period_begin_year = year
        period_end_year = year

        fiscal_year_end = params["FiscalYearEnd"]
        fiscal_year_end_obj = datetime.datetime.strptime(fiscal_year_end, '%m-%d')

        fiscal_quarter_end_list = []

        for delta in range(1, 5):
            ym = fiscal_year_end_obj.month + 3*delta
            if ym > 12:
                ym = ym - 12
            fiscal_quarter_end_list.append(ym)

        most_recent_fiscal_quarter_end_month = fiscal_year_end_obj.month
        for item in fiscal_quarter_end_list:
            for i in range(1,4):
                temp_month = item + i
                if temp_month > 12:
                    temp_month = 12 - temp_month
                    if temp_month == month:
                        most_recent_fiscal_quarter_end_month = temp_month
                        period_end_year = year - 1
                elif temp_month == month:
                    most_recent_fiscal_quarter_end_month = item

        most_recent_fiscal_quarter_begin_month = most_recent_fiscal_quarter_end_month - 3

        if most_recent_fiscal_quarter_begin_month <= 0:
            most_recent_fiscal_quarter_begin_month = most_recent_fiscal_quarter_begin_month + 12
            period_begin_year = period_begin_year - 1

        res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, most_recent_fiscal_quarter_begin_month)], 30)
        res_end = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, most_recent_fiscal_quarter_end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=most_recent_fiscal_quarter_begin_month, day=res_begin)
        period_end_date = date_obj_end.replace(year=period_end_year, month=most_recent_fiscal_quarter_end_month, day=res_end)

    if period_code.startswith("PFQ"):
        # Calculate most recent fiscal quarter first
        month = date_obj_begin.month
        year = date_obj_begin.year

        period_begin_year = year
        period_end_year = year

        fiscal_year_end = params["FiscalYearEnd"]
        fiscal_year_end_obj = datetime.datetime.strptime(fiscal_year_end, '%m-%d')

        fiscal_quarter_end_list = []

        for delta in range(1, 5):
            ym = fiscal_year_end_obj.month + 3 * delta
            if ym > 12:
                ym = ym - 12
            fiscal_quarter_end_list.append(ym)

        most_recent_fiscal_quarter_end_month = fiscal_year_end_obj.month
        for item in fiscal_quarter_end_list:
            for i in range(1, 4):
                temp_month = item + i
                if temp_month > 12:
                    temp_month = 12 - temp_month
                    if temp_month == month:
                        most_recent_fiscal_quarter_end_month = temp_month
                        period_end_year = year - 1
                elif temp_month == month:
                    most_recent_fiscal_quarter_end_month = item

        most_recent_fiscal_quarter_begin_month = most_recent_fiscal_quarter_end_month - 3

        if most_recent_fiscal_quarter_begin_month <= 0:
            most_recent_fiscal_quarter_begin_month += 12
            period_begin_year = period_begin_year - 1

        ###
        ## Calculate the prior fiscal quarter periods
        period_details = period_universe[period_code]
        num_quarters = int(period_details['Quarters'])

        most_recent_fiscal_quarter_end_month = most_recent_fiscal_quarter_end_month - 3*num_quarters
        if most_recent_fiscal_quarter_end_month <=0 :
            most_recent_fiscal_quarter_end_month += 12
            period_end_year -= 1

        most_recent_fiscal_quarter_begin_month = most_recent_fiscal_quarter_end_month - 3
        if most_recent_fiscal_quarter_begin_month <=0 :
            most_recent_fiscal_quarter_begin_month += 12
            period_begin_year -= 1

        res_begin = ft.reduce(lambda x, y: y[1],
                              [calendar.monthrange(period_begin_year, most_recent_fiscal_quarter_begin_month)], 30)
        res_end = ft.reduce(lambda x, y: y[1],
                            [calendar.monthrange(period_end_year, most_recent_fiscal_quarter_end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=most_recent_fiscal_quarter_begin_month,
                                                   day=res_begin)
        period_end_date = date_obj_end.replace(year=period_end_year, month=most_recent_fiscal_quarter_end_month,
                                               day=res_end)


    if period_code.startswith("PFY"):
        period_details = period_universe[period_code]
        num_years = int(period_details['Years'])

        fiscal_year_end = params["FiscalYearEnd"]
        fiscal_year_end_obj = datetime.datetime.strptime(fiscal_year_end, '%m-%d')

        year = date_obj_begin.year

        begin_year = year - num_years - 1
        begin_month = fiscal_year_end_obj.month

        end_year = year - num_years
        end_month = fiscal_year_end_obj.month

        end_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(end_year, end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=begin_year, month=begin_month, day=end_day)
        period_end_date = date_obj_end.replace(year=end_year, month=end_month, day=end_day)

    if period_code == "FYTD":
        month = date_obj_begin.month
        year = date_obj_begin.year

        fiscal_year_end = params["FiscalYearEnd"]
        fiscal_year_end_obj = datetime.datetime.strptime(fiscal_year_end, '%m-%d')

        period_begin_month = fiscal_year_end_obj.month
        period_begin_day = fiscal_year_end_obj.day
        period_begin_year = year

        period_end_month = month
        if period_end_month <= period_begin_month:
            period_begin_year = year - 1

        period_begin_date = date_obj_begin.replace(year = period_begin_year, month = period_begin_month, day = period_begin_day)
        period_end_date = date_obj_end

    if period_code.endswith("MT"):
        period_details = period_universe[period_code]
        num_months = int(period_details['Months'])

        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year

        res_end = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)
        if res_end == day:
            period_begin_month = period_begin_month - num_months
            if period_begin_month <= 0:
                period_begin_year = period_begin_year - 1
                period_begin_month = 12 + period_begin_month
            res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res_begin)
        else:
            period_begin_month = period_begin_month - num_months
            if period_begin_month <= 0:
                period_begin_year = period_begin_year - 1
                period_begin_month = 12 + period_begin_month
            res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            if period_begin_month == 2:
                if day > res_begin:
                    period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                               day=res_begin)
                else:
                    period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                               day=day)
            else:
                period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                               day=day)
        period_end_date = date_obj_end

    if period_code.endswith("YC") or period_code.endswith("YA"):
        period_details = period_universe[period_code]
        num_years = int(period_details['Years'])

        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year

        res_end = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)
        if res_end == day:
            period_begin_year = period_begin_year - num_years
            res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month, day=res_begin)
        else:
            period_begin_year = period_begin_year - num_years
            res_begin = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            if period_begin_month == 2:
                if day > res_begin:
                    period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                               day=res_begin)
                else:
                    period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                               day=day)
            else:
                period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                           day=day)
        period_end_date = date_obj_end

    if period_code == "ITDA" or period_code == "ITD":
        date_obj_begin = datetime.datetime.strptime(params["InceptionDate"], '%Y-%m-%d')
        period_begin_date = date_obj_begin
        period_end_date = date_obj_end

    if period_code.startswith("MRM"):
        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year
        period_end_day = day

        if period_end_day == ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30):
            period_begin_month -= 1
            if period_begin_month == 0:
                period_begin_year -= 1
                period_begin_month = 12
            period_begin_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_begin_year, period_begin_month)], 30)
        else:
            period_begin_month -= 2
            period_end_month -= 1
            if period_begin_month <= 0:
                period_begin_year -= 1
                period_begin_month = 12 + period_begin_month
            period_begin_day = ft.reduce(lambda x, y: y[1],
                                         [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            if period_end_month == 0:
                period_end_year -= 1
                period_end_month = 12
            period_end_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                   day=period_begin_day)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month,
                                                   day=period_end_day)

    if period_code.startswith("PM"):
        period_details = period_universe[period_code]
        num_months = int(period_details['Months'])

        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_month = month
        period_begin_year = year

        period_end_month = month
        period_end_year = year
        period_end_day = day

        if period_end_day == ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30):
            period_begin_month -= (num_months + 1)
            period_end_month -= (num_months)
            if period_begin_month <= 0:
                period_begin_year -= 1
                period_begin_month += 12
            period_begin_day = ft.reduce(lambda x, y: y[1],
                                         [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            if period_end_month <= 0:
                period_end_year -= 1
                period_end_month = 12
            period_end_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)
        else:
            period_begin_month -= (num_months + 2)
            period_end_month -= (num_months + 1)
            if period_begin_month <= 0:
                period_begin_year -= 1
                period_begin_month = 12 + period_begin_month
            period_begin_day = ft.reduce(lambda x, y: y[1],
                                         [calendar.monthrange(period_begin_year, period_begin_month)], 30)
            if period_end_month == 0:
                period_end_year -= 1
                period_end_month = 12
            period_end_day = ft.reduce(lambda x, y: y[1], [calendar.monthrange(period_end_year, period_end_month)], 30)

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                   day=period_begin_day)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month,
                                               day=period_end_day)
    if period_code.startswith("MRY"):
        month = date_obj_begin.month
        year = date_obj_begin.year
        day = date_obj_begin.day

        period_begin_year = year
        period_begin_month = month

        period_end_year = year
        period_end_month = month
        period_end_day = day

        if period_end_month == 12 and period_end_day == 31:
            period_begin_year -= 1
        else:
            period_begin_year -= 2
            period_end_year -= 1
            period_begin_month = 12
            period_end_month = 12

        period_begin_date = date_obj_begin.replace(year=period_begin_year, month=period_begin_month,
                                                   day=31)
        period_end_date = date_obj_end.replace(year=period_end_year, month=period_end_month,
                                               day=31)

    try:
        period_begin_date = period_begin_date.strftime('%Y-%m-%d')
        period_end_date = period_end_date.strftime('%Y-%m-%d')

        response['data'] = [{"begin_date": period_begin_date, "end_date": period_end_date}]
    except Exception as err:
        print(err, period_begin_date, period_end_date, period_code)


def test_get_begin_date():
    parameters = {"AsofDate": "2023-04-30T00:00:00", "PeriodCode":"MRY", "FiscalYearEnd": "04-30", "InceptionDate": "2015-09-08", "DatePattern": "%Y-%m-%dT%H:%M:%S"}
    response = {}
    exec_script(response, read_data, parameters)
    print(parameters["PeriodCode"], parameters["AsofDate"], response)
    parameters = {"AsofDate": "2023-04-30", "PeriodCode":"PY1", "FiscalYearEnd": "04-30", "InceptionDate": "2015-09-08"}
    response = {}
    exec_script(response, read_data, parameters)
    print(parameters["PeriodCode"], parameters["AsofDate"], response)
    parameters = {"AsofDate": "2023-12-31", "PeriodCode":"MRY", "FiscalYearEnd": "04-30", "InceptionDate": "2015-09-08"}
    response = {}
    exec_script(response, read_data, parameters)
    print(parameters["PeriodCode"], parameters["AsofDate"], response)
    parameters = {"AsofDate": "2021-04-30", "PeriodCode":"PM3", "FiscalYearEnd": "04-30", "InceptionDate": "2015-09-08"}
    response = {}
    exec_script(response, read_data, parameters)
    print(parameters["PeriodCode"], parameters["AsofDate"], response)
    parameters = {"AsofDate": "2021-12-15", "PeriodCode":"MRM", "FiscalYearEnd": "04-30", "InceptionDate": "2015-09-08"}
    response = {}
    exec_script(response, read_data, parameters)
    print(parameters["PeriodCode"], parameters["AsofDate"], response)


# test_get_begin_date()
def external_call(params):

    parameters = params
    response = {}
    exec_script(response, read_data, parameters)
    return response


# def get_last_month_end():
#     now = datetime.datetime.now()
#     # Subtract the number of days since the start of the month to get to the first day of the month
#     first_day_of_current_month = now.replace(day=1)
#     # Subtract one day to get to the last day of the previous month
#     last_month_end = first_day_of_current_month - datetime.timedelta(days=1)
#     return last_month_end.date()

# last_month_end = get_last_month_end()
# print(last_month_end)