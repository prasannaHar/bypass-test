import logging
import random
import string
from datetime import datetime, time, timedelta, date
import pytz
import requests
from pytz import timezone
from dateutil.relativedelta import relativedelta
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class WidgetReusable:
    def __init__(self, generic_helper):
        self.generic = generic_helper

    def retrieve_required_api_response(self, arg_req_api, arg_req_payload, request_type="post"):
        """this function will be responsible for retrieving widget api response
        for the given parameters

        Args:
            request_type:
            arg_req_api (string): required api url
            arg_req_payload (json object): json payload needs to be passed

        Returns:
            Json: Widget data json response
        """
        response = self.generic.execute_api_call(arg_req_api, request_type, data=arg_req_payload)
        return response

    def retrieve_three_random_records(self, arg_inp_json_response, number_records_required=3):
        if "count" in arg_inp_json_response:
            drill_down_records_count = arg_inp_json_response["count"]
        else:
            return []
        if drill_down_records_count <= 3:
            required_records = arg_inp_json_response['records']
        else:
            temp_records = arg_inp_json_response['records']
            rcrd1 = random.randint(0, len(temp_records) - 1)
            rcrd2 = random.randint(0, len(temp_records) - 1)
            rcrd3 = random.randint(0, len(temp_records) - 1)
            required_records = [temp_records[rcrd1], temp_records[rcrd2], temp_records[rcrd3]]
        return required_records[:number_records_required]

    def widget_retrieve_f(self, arg_inp_json_response):

        drill_down_records_count = 0
        if "count" in arg_inp_json_response:
            drill_down_records_count = arg_inp_json_response["count"]
        else:
            return []
        if drill_down_records_count <= 3:
            return arg_inp_json_response['records']
        else:
            temp_records = arg_inp_json_response['records']
            rcrd1 = random.randint(0, len(temp_records))
            rcrd2 = random.randint(0, len(temp_records))
            rcrd3 = random.randint(0, len(temp_records))
            temp_rslt_rcrds = [temp_records[rcrd1], temp_records[rcrd2], temp_records[rcrd3]]
            return temp_rslt_rcrds

    def get_three_random_months(self, arg_gt, arg_lt, arg_months):
        random_months = [arg_gt]
        for x in range(arg_months):
            datetime.combine(arg_gt + relativedelta(months=x + 1), time.min)
            if datetime.combine(arg_gt + relativedelta(months=x + 1), time.min) < arg_lt:
                random_months.append(datetime.combine(arg_gt + relativedelta(months=x + 1), time.min))
        del random_months[-1]
        sampled_list = random.sample(random_months, min(len(random_months), 3))
        return sampled_list

    def get_three_random_qtrs(self, arg_gt, arg_lt):
        gtQuarter = (arg_gt.month - 1) / 3 + 1
        QTRfirst_date = datetime(arg_gt.year, 3 * int(gtQuarter) - 2, 1)
        QTRfirst_date_epoc = int(QTRfirst_date.timestamp() + 19800)
        QTRlast_date = datetime(arg_gt.year, 3 * int(gtQuarter) + 1, 1) + timedelta(days=-1)
        QTRlast_date = int(QTRlast_date.timestamp() + 19800)

        random_qtr = [QTRfirst_date]
        while QTRfirst_date < arg_lt:
            random_qtr.append(datetime.combine(QTRfirst_date + relativedelta(months=3), time.min))
            QTRfirst_date = QTRfirst_date + relativedelta(months=3)

        del random_qtr[-1]

        sampled_list = random.sample(random_qtr, min(3, len(random_qtr)))
        return sampled_list

    def random_text_generators(self, arg_length=15,
                               arg_only_digits=False,
                               arg_only_text=False
                               ):
        """this function return the random text start with py_auto

        Args:
            arg_length (int, optional): required string lenght. Defaults to 15.
            arg_only_digits (bool, optional): only digits flag. Defaults to False.
            arg_only_text (bool, optional): only text flag. Defaults to False.

        Returns:
            str: required random string
        """
        digits = ''.join(random.sample(string.digits, arg_length))
        chars = ''.join(random.sample(string.ascii_lowercase, arg_length))
        if arg_only_digits:
            return str(digits)
        elif arg_only_text:
            return str(chars)
        else:
            digits_length = int(arg_length / 4)
            text_lenght = arg_length - digits_length
            return chars[:digits_length] + digits[:text_lenght]

    def epoch_timeStampsGenerationForRequiredTimePeriods(self, arg_required_time_period):
        """this function returns epoch timestamps for the required timeperiod.

           currently this function is implemented only for last month/last quater/last 2 quarters/last year

        Args:
            arg_required_time_period (string): "required timeperiod - allowed values are LAST_YEAR,LAST_QUARTER,LAST_TWO_QUARTERS,LAST_MONTH
            Note: values are case-sensitive"

        Returns:
            epoch start time stamp, epoch end time stamp: this function will return the start day and end day epoch times of selected timeperiods
        """
        if arg_required_time_period == "LAST_MONTH":
            last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
            start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
            LOG.info(type(last_day_of_prev_month))
            start_day_of_prev_month = datetime.combine(start_day_of_prev_month, datetime.min.time())
            last_day_of_prev_month = datetime.combine(last_day_of_prev_month, datetime.max.time())
            LOG.info("First day of prev month:{}".format(start_day_of_prev_month))
            LOG.info("Last day of prev month:{}".format(last_day_of_prev_month))
            start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
            start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
            LOG.info(start_day_of_prev_month)
            last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
            last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
            LOG.info(last_day_of_prev_month)
            return 1, start_day_of_prev_month, last_day_of_prev_month

        elif arg_required_time_period == "LAST_QUARTER":
            current_date = datetime.now()
            current_quarter = round((current_date.month - 1) // 3 + 1)
            LOG.info(current_quarter)
            first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
            LOG.info("First Day of Quarter:{}".format(first_date))
            last_quarter_first_date = first_date - relativedelta(months=3)
            last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('GMT'))
            last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
            LOG.info(last_quarter_first_date)
            last_quarter_last_date = first_date - timedelta(days=1)
            last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
            last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('GMT'))
            last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
            LOG.info(last_quarter_last_date)
            return 3, last_quarter_first_date, last_quarter_last_date
        elif arg_required_time_period == "LAST_TWO_QUARTERS":
            LOG.info("")
            current_date = datetime.now()
            current_quarter = round((current_date.month - 1) // 3 + 1)
            LOG.info(current_quarter)
            first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
            LOG.info("First Day of Quarter:{}".format(first_date))
            last_quarter_first_date = first_date - relativedelta(months=6)
            last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('GMT'))
            last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
            LOG.info(last_quarter_first_date)
            last_quarter_last_date = first_date - timedelta(days=1)
            last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
            last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('GMT'))
            last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
            LOG.info(last_quarter_last_date)
            return 6, last_quarter_first_date, last_quarter_last_date
        elif arg_required_time_period == "LAST_YEAR":
            LOG.info("")

            year = 2021
            first_day_of_year = date.min.replace(year=year)
            last_day_of_year = date.max.replace(year=year)
            LOG.info(first_day_of_year, last_day_of_year)
            start_day_of_prev_month = datetime.combine(first_day_of_year, datetime.min.time())
            last_day_of_prev_month = datetime.combine(last_day_of_year, datetime.max.time())
            start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
            start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
            LOG.info(start_day_of_prev_month)
            last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
            last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
            LOG.info(last_day_of_prev_month)
            return 12, start_day_of_prev_month, last_day_of_prev_month
        else:
            return "<Logic not yet implemented>"

    def epoch_timeStampsGenerationForRequiredTimePeriods_utc(self, arg_required_time_period):
        """this function returns epoch timestamps for the required timeperiod.

           currently this function is implemented only for last month/last quater/last 2 quarters/last year

        Args:
            arg_required_time_period (string): "required timeperiod - allowed values are LAST_YEAR,LAST_QUARTER,LAST_TWO_QUARTERS,LAST_MONTH
            Note: values are case-sensitive"

        Returns:
            epoch start time stamp, epoch end time stamp: this function will return the start day and end day epoch times of selected timeperiods
        """
        if arg_required_time_period == "LAST_MONTH":
            last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
            start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
            LOG.info(type(last_day_of_prev_month))
            start_day_of_prev_month = datetime.combine(start_day_of_prev_month, datetime.min.time())
            last_day_of_prev_month = datetime.combine(last_day_of_prev_month, datetime.max.time())
            LOG.info("First day of prev month:{}".format(start_day_of_prev_month))
            LOG.info("Last day of prev month:{}".format(last_day_of_prev_month))
            start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('UTC'))
            start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
            LOG.info(start_day_of_prev_month)
            last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('UTC'))
            last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
            LOG.info(last_day_of_prev_month)
            return 1, start_day_of_prev_month, last_day_of_prev_month

        elif arg_required_time_period == "LAST_QUARTER":
            current_date = datetime.now()
            current_quarter = round((current_date.month - 1) // 3 + 1)
            LOG.info(current_quarter)
            first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
            LOG.info("First Day of Quarter:{}".format(first_date))
            last_quarter_first_date = first_date - relativedelta(months=3)
            last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('UTC'))
            last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
            LOG.info(last_quarter_first_date)
            last_quarter_last_date = first_date - timedelta(days=1)
            last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
            last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('UTC'))
            last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
            LOG.info(last_quarter_last_date)
            return 3, last_quarter_first_date, last_quarter_last_date
        elif arg_required_time_period == "LAST_TWO_QUARTERS":
            LOG.info("")
            current_date = datetime.now()
            current_quarter = round((current_date.month - 1) // 3 + 1)
            LOG.info(current_quarter)
            first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
            LOG.info("First Day of Quarter:{}".format(first_date))
            last_quarter_first_date = first_date - relativedelta(months=6)
            last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('UTC'))
            last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
            LOG.info(last_quarter_first_date)
            last_quarter_last_date = first_date - timedelta(days=1)
            last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
            last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('UTC'))
            last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
            LOG.info(last_quarter_last_date)
            return 6, last_quarter_first_date, last_quarter_last_date
        elif arg_required_time_period == "LAST_YEAR":
            LOG.info("")

            year = 2021
            first_day_of_year = date.min.replace(year=year)
            last_day_of_year = date.max.replace(year=year)
            LOG.info(first_day_of_year, last_day_of_year)
            start_day_of_prev_month = datetime.combine(first_day_of_year, datetime.min.time())
            last_day_of_prev_month = datetime.combine(last_day_of_year, datetime.max.time())
            start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('UTC'))
            start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
            LOG.info(start_day_of_prev_month)
            last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('UTC'))
            last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
            LOG.info(last_day_of_prev_month)
            return 12, start_day_of_prev_month, last_day_of_prev_month
        else:
            return "<Logic not yet implemented>"

    def call_required_apis(self, arg_authorization_token, arg_req_api, arg_request_type, arg_req_payload):
        """this function will be responsible for retrieving widget api response
        for the given parameters

        Args:
            arg_authorization_token (string): application authorization token
            arg_req_api (string): required api url
            arg_req_payload (json object): json payload needs to be passed

        Returns:
            Json: Widget data json response
        """
        headers = {
            'Authorization': arg_authorization_token,
            'Content-Type': 'application/json'
        }
        response = requests.request(arg_request_type, arg_req_api, headers=headers, data=arg_req_payload)
        assert response.status_code == 200, response
        api_call_status = True
        if response.status_code != 200:
            api_call_status = False
        try:
            response_json_format = response.json()
        except:
            response_json_format = response
        return api_call_status, response_json_format

    def get_start_end_date_month_from_a_given_date(self, given_epoch):
        # convert epoch to datetime object in UTC
        given_date = datetime.utcfromtimestamp(given_epoch)

        # create a timezone object for GMT
        gmt = pytz.timezone('GMT')

        # localize the datetime object to GMT timezone
        given_date_gmt = gmt.localize(given_date)

        # get the start of the month in GMT timezone
        start_of_month_gmt = given_date_gmt.replace(day=1)

        # get the end of the month in GMT timezone
        next_month_gmt = start_of_month_gmt.replace(month=start_of_month_gmt.month + 1, day=1)
        end_of_month_gmt = next_month_gmt - datetime.timedelta(days=1)

        # convert start and end dates to GMT epoch format
        start_of_month_gmt_epoch = int(start_of_month_gmt.timestamp())
        end_of_month_gmt_epoch = int(end_of_month_gmt.timestamp())
        return start_of_month_gmt_epoch, end_of_month_gmt_epoch

    def get_all_month_start_date_between_2_dates(self, start_epoch, end_epoch):
        # create datetime objects from epoch timestamps
        start_date = datetime.fromtimestamp(start_epoch)
        end_date = datetime.fromtimestamp(end_epoch)

        # initialize the current month as the start month
        current_month = start_date.month

        # initialize the list of month start dates
        month_start_dates = []

        # loop through all months between the start and end dates
        while start_date < end_date:
            # get the start date of the current month
            month_start = datetime(start_date.year, current_month, 1)

            # add the month start date to the list
            month_start_dates.append(int(month_start.timestamp()))

            # increment the current month
            current_month += 1

            # if the current month is greater than 12, wrap around to January of the next year
            if current_month > 12:
                current_month = 1
                start_date = datetime(start_date.year + 1, 1, 1)
            else:
                start_date = datetime(start_date.year, current_month, 1)

        # return the list of month start dates
        return month_start_dates

    def get_day_start_end_dates(self, epoch_time):
        """
        Function to calculate the start and end of the day based on the epoch time provided.
        Args:
        epoch_time (int): Time in epoch format (seconds).

        Returns: Object containing start and end time of the day.
        """

        date_obj = datetime.fromtimestamp(epoch_time)
        start_time = datetime.combine(date_obj, time.min)
        end_time = datetime.combine(date_obj, time.max)

        # Converting to the GMT format.
        start_time = int(start_time.replace(tzinfo=timezone("GMT")).timestamp())
        end_time = int(end_time.replace(tzinfo=timezone("GMT")).timestamp())

        return {"$gt": start_time, "$lt": end_time}

    def get_last_day_of_month(self, epoch_time):
        # convert epoch time to datetime object
        date = datetime.fromtimestamp(epoch_time)

        # get the last day of the current month
        last_day = date.replace(day=28) + timedelta(days=4)
        last_day = last_day - timedelta(days=last_day.day)

        # convert last day to epoch time
        last_day = datetime.combine(last_day, time.max)
        last_day_epoch=int(last_day.replace(tzinfo=timezone("GMT")).timestamp())
        # last_day_epoch = int(last_day.timestamp()) + 106199
        # 86399 +19800

        # return last day epoch time
        return last_day_epoch

    def get_week_start_dates(self, start_epoch, end_epoch):
        week_start_dates = []
        current_week_start = self.get_week_start_date(start_epoch)
        while current_week_start < end_epoch:
            week_start_dates.append(current_week_start)
            current_week_start += 7 * 24 * 60 * 60  # add 1 week in seconds
        return week_start_dates

    def get_week_start_date(self, epoch_time):
        # convert epoch time to datetime object
        date = datetime.fromtimestamp(epoch_time)

        # calculate the number of days to subtract to get to the start of the week
        days_to_subtract = date.weekday()

        # get the start date of the week
        week_start = date - timedelta(days=days_to_subtract)

        # convert week start to epoch time
        week_start_epoch = int(week_start.timestamp())

        # return week start epoch time
        return week_start_epoch

    def get_week_end_date(self, epoch_time, get_day_end_time=False):
        # convert epoch time to datetime object
        date = datetime.fromtimestamp(int(epoch_time))

        # calculate the number of days to add to get to the end of the week
        days_to_add = 6 - date.weekday()

        # get the end date of the week
        week_end = date + timedelta(days=days_to_add)
        if get_day_end_time:
            week_end = datetime.combine(week_end, time.max)

        # convert week end to epoch time
        week_end_epoch = int(week_end.timestamp())

        # return week end epoch time
        return week_end_epoch

    def retrive_mean_median_p90_p95(self, response_data, key_name, key, record_path="data",
                                    tc_name=None):
        record_count = int(response_data['count'])
        if record_count == 0:
            return {"mean": 0, "median": 0, "p95": 0, "p90": 0}
        if record_path is not None:
            response_data_df = pd.json_normalize(response_data['records'], record_path=record_path)
        else:
            response_data_df = pd.json_normalize(response_data['records'])

        response_data_df = response_data_df[response_data_df[key] == key_name]
        response_data_df = response_data_df.fillna(0)
        if tc_name == "issue_lead_time_by_time":
            mean = response_data_df['time_spent'].sum() / record_count
            median = response_data_df['time_spent'].median()
            p95 = response_data_df["time_spent"].quantile(0.95)
            p90 = response_data_df["time_spent"].quantile(0.90)
            return {"mean": mean, "median": int(median), "p95": int(p95), "p90": int(p90)}
        else:
            mean = response_data_df["mean"].sum() / record_count
            median_values = response_data_df["median"].values.tolist()
            median_index = int(len(median_values) / 2)
            median = median_values[median_index]
            p95 = response_data_df["mean"].quantile(0.95)
            p90 = response_data_df["mean"].quantile(0.90)
            return {"mean": mean, "median": median, "p95": p95, "p90": p90}

    def compare_mean_median_p0_p95(self, org_id, key, widget_data, drilldown_data):
        mismatch_data_ous_list = []
        # breakpoint()
        if (widget_data['mean'] - drilldown_data['mean']) != 0:
            mismatch_data_ous_list.append(
                org_id + "_" + key + "_mean" + " difference----" + str(widget_data['mean'] - drilldown_data['mean']))
        if not (-(3600 * 4) <= (widget_data['median'] - drilldown_data['median']) <= (3600 * 4)):
            mismatch_data_ous_list.append(org_id + "_" + key + "_median" + " difference----" + str(
                widget_data['median'] - drilldown_data['median']))
        if not (-60 <= (widget_data['p90'] - drilldown_data['p90']) <= 60):
            mismatch_data_ous_list.append(
                org_id + "_" + key + "_p90" + " difference----" + str(widget_data['p90'] - drilldown_data['p90']))
        if not (-60 <= (widget_data['p95'] - drilldown_data['p95']) <= 60):
            mismatch_data_ous_list.append(
                org_id + "_" + key + "_p95" + " difference----" + str(widget_data['p95'] - drilldown_data['p95']))
        return mismatch_data_ous_list
