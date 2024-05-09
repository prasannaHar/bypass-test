import logging
import datetime
from pytz import timezone
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class DateTimeReusable:

    def get_month_range(self, month_year_str):
        month_number, year = month_year_str.split("-")
        start_month = int(month_number)
        year = int(year)
        # Get the start and end dates of the quarter
        month_start_date = datetime.date(year, start_month, 1)
        month_end_date = datetime.date(year, start_month, 1) + datetime.timedelta(days=31)
        month_end_date = month_end_date.replace(day=1) - datetime.timedelta(days=1)
        # Convert the dates to GMT timezone
        gmt = datetime.timezone(datetime.timedelta(hours=0))
        quarter_start_date_gmt = datetime.datetime.combine(month_start_date, datetime.time.min, gmt)
        quarter_end_date_gmt = datetime.datetime.combine(month_end_date, datetime.time.max, gmt)
        # Convert the dates to epoch timestamps
        quarter_start_epoch = int(quarter_start_date_gmt.timestamp())
        quarter_end_epoch = int(quarter_end_date_gmt.timestamp())
        return str(quarter_start_epoch), str(quarter_end_epoch)

    def get_week_range(self, weekstart_date):
        from datetime import datetime, timedelta
        date = datetime.fromtimestamp(int(weekstart_date))
        days_to_add = 7 - date.weekday()
        week_end = date + timedelta(days=days_to_add)
        return str(weekstart_date), str(int(week_end.timestamp()) - 1)

    def get_biweek_range(self, weekstart_date):
        from datetime import datetime, timedelta
        date = datetime.fromtimestamp(int(weekstart_date))
        days_to_add = 14 - date.weekday()
        week_end = date + timedelta(days=days_to_add)
        return str(weekstart_date),str(int(week_end.timestamp())-1)

    def get_quarter_range(self, quarter_year_str):
        # Parse the quarter and year string to get the quarter number and year
        quarter_number, year = quarter_year_str.split("-")
        quarter_number = int(quarter_number[1:])
        year = int(year)
        # Define the start and end months of the quarter
        start_month = (quarter_number - 1) * 3 + 1
        end_month = start_month + 2
        # Get the start and end dates of the quarter
        quarter_start_date = datetime.date(year, start_month, 1)
        quarter_end_date = datetime.date(year, end_month, 1) + datetime.timedelta(days=31)
        quarter_end_date = quarter_end_date.replace(day=1) - datetime.timedelta(days=1)
        # Convert the dates to GMT timezone
        gmt = datetime.timezone(datetime.timedelta(hours=0))
        quarter_start_date_gmt = datetime.datetime.combine(quarter_start_date, datetime.time.min, gmt)
        quarter_end_date_gmt = datetime.datetime.combine(quarter_end_date, datetime.time.max, gmt)
        # Convert the dates to epoch timestamps
        quarter_start_epoch = int(quarter_start_date_gmt.timestamp())
        quarter_end_epoch = int(quarter_end_date_gmt.timestamp())
        return str(quarter_start_epoch), str(quarter_end_epoch)

    def get_last_month_epochdate_and_date_range(self):
        last_day_of_prev_month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
        start_day_of_prev_month = datetime.date.today().replace(day=1) - datetime.timedelta(
            days=last_day_of_prev_month.day)
        start_day_of_prev_month = datetime.datetime.combine(start_day_of_prev_month, datetime.datetime.min.time())
        last_day_of_prev_month = datetime.datetime.combine(last_day_of_prev_month, datetime.datetime.max.time())
        start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        start_day_of_prev_month_date = start_day_of_prev_month.strftime('%Y-%m-%d')
        start_day_of_prev_month_epoch = str(int(float(start_day_of_prev_month.timestamp())))
        last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        last_day_of_prev_month_date = last_day_of_prev_month.strftime('%Y-%m-%d')
        last_day_of_prev_month_epoch = str(int(float(last_day_of_prev_month.timestamp())))
        return [start_day_of_prev_month_epoch, last_day_of_prev_month_epoch], [start_day_of_prev_month_date,
                                                                               last_day_of_prev_month_date]

    def convert_epochtime_to_github_api_datetime_format(self, input_val):
        if pd.notnull(input_val):
            return datetime.datetime.utcfromtimestamp(input_val).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            return input_val

    def convert_github_api_datetime_format_to_epochtime(self, input_val):
        if pd.notnull(input_val):
            return datetime.datetime.strptime(input_val, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000
        else:
            return input_val

    def get_last_week_epochdate_and_date_range(self, last_n_weeks=1):

        current_datetime = datetime.datetime.now()
        # Calculate the start date and time of the last week
        start_of_last_week = current_datetime - datetime.timedelta(days=current_datetime.weekday() + (7*last_n_weeks))
        start_of_last_week = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_last_week = start_of_last_week.replace(tzinfo=timezone('GMT'))
        # Calculate the end date and time of the last week
        end_of_last_week = start_of_last_week + datetime.timedelta(days=6, hours=23, minutes=59, seconds=59)
        end_of_last_week = end_of_last_week.replace(tzinfo=timezone('GMT'))


        start_day_of_prev_month_date = start_of_last_week.strftime('%Y-%m-%d')
        start_day_of_prev_month_epoch = str(int(float(start_of_last_week.timestamp())))

        last_day_of_prev_month_date = end_of_last_week.strftime('%Y-%m-%d')
        last_day_of_prev_month_epoch = str(int(float(end_of_last_week.timestamp())))

        return [start_day_of_prev_month_epoch, last_day_of_prev_month_epoch], [start_day_of_prev_month_date,
                                                                               last_day_of_prev_month_date]
    def get_biweekly_range(self, epoch_date):
        # Convert epoch date to a datetime object
        date = datetime.datetime.utcfromtimestamp(epoch_date)

        # Define the duration of a biweekly period (14 days)
        biweekly_duration = datetime.timedelta(days=14)

        # Calculate the start of the biweekly period containing the given date
        start_of_biweekly = date - datetime.timedelta(days=date.weekday())  # Start of the week
        start_of_biweekly -= datetime.timedelta(
            days=(start_of_biweekly.isocalendar()[2] - 1) % 14)  # Adjust to biweekly

        # Calculate the end of the biweekly period with time set to 23:59:59
        end_of_biweekly = start_of_biweekly + biweekly_duration - datetime.timedelta(seconds=1)

        # Convert date objects to epoch timestamps
        start_date_epoch = int(start_of_biweekly.timestamp())
        end_date_epoch = int(end_of_biweekly.timestamp())

        return start_date_epoch, end_date_epoch

    def convert_date_in_epoch(self, date_str, date_format="%Y-%m-%dT%H:%M:%S.%fZ"):
        """
        Convert date string into epoch format.
        Args:
        datestr: Date-time in string format.
        date_format: Format of date-time in string format.

        Returns: Date in epoch milliseconds.
        """

        if not date_str:
            return 0

        date = datetime.datetime.strptime(date_str, date_format)
        date_epoch = date.timestamp() * 1000

        return date_epoch

    def get_time_in_expected_format(self, epoch_time, date_format="%Y-%m-%dT%H:%M:%S.%f"):
        """
        Return date in ISO 8601 format. (YYYY-MM-DDTHH:mm:ss.fffZ)
        Args:
        epoch_time: Date in epoch time.
        date_format: Format to convert the date object into.

        Returns: Date-time in expected format.
        """

        date = datetime.datetime.fromtimestamp(int(epoch_time))
        date_in_format = date.strftime(date_format)
        date_in_format = date_in_format[:-3] + "Z"
        return date_in_format