from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta, datetime
import time


def epoch_timeStampsGenerationForRequiredTimePeriods(arg_required_time_period):
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
        start_day_of_prev_month = datetime.combine(start_day_of_prev_month, datetime.min.time())
        last_day_of_prev_month = datetime.combine(last_day_of_prev_month, datetime.max.time())
        start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
        last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
        return 1, str(start_day_of_prev_month), str(last_day_of_prev_month)

    if arg_required_time_period=="LAST_TWO_MONTHS":
        ## last day of previous month
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        last_day_of_prev_month = datetime.combine(last_day_of_prev_month, datetime.max.time())
        last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
        ## start day of previous previous month
        start_day_of_prev_month = date.today().replace(day=1) - relativedelta(months=2)
        start_day_of_prev_month = datetime.combine(start_day_of_prev_month, datetime.min.time())
        start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
        return 2, str(start_day_of_prev_month), str(last_day_of_prev_month)

    if arg_required_time_period=="LAST_THREE_MONTHS":
        ## last day of previous month
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        last_day_of_prev_month = datetime.combine(last_day_of_prev_month, datetime.max.time())
        last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
        ## start day of previous previous month
        start_day_of_prev_month = date.today().replace(day=1) - relativedelta(months=3)
        start_day_of_prev_month = datetime.combine(start_day_of_prev_month, datetime.min.time())
        start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
        return 3, str(start_day_of_prev_month), str(last_day_of_prev_month)

    elif arg_required_time_period=="LAST_QUARTER":
        current_date = datetime.now()
        current_quarter = round((current_date.month - 1) // 3 + 1)
        first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
        last_quarter_first_date = first_date - relativedelta(months=3)
        last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('GMT'))
        last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
        last_quarter_last_date = first_date - timedelta(days=1)
        last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
        last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('GMT'))
        last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
        return 3, str(last_quarter_first_date), str(last_quarter_last_date)

    elif arg_required_time_period == "LAST_TWO_QUARTERS":
        current_date = datetime.now()
        current_quarter = round((current_date.month - 1) // 3 + 1)
        first_date = datetime(current_date.year, 3 * current_quarter - 2, 1)
        last_quarter_first_date = first_date - relativedelta(months=6)
        last_quarter_first_date = last_quarter_first_date.replace(tzinfo=timezone('GMT'))
        last_quarter_first_date = int(float(last_quarter_first_date.timestamp()))
        last_quarter_last_date = first_date - timedelta(days=1)
        last_quarter_last_date = datetime.combine(last_quarter_last_date.date(), datetime.max.time())
        last_quarter_last_date = last_quarter_last_date.replace(tzinfo=timezone('GMT'))
        last_quarter_last_date = int(float(last_quarter_last_date.timestamp()))
        return 6, str(last_quarter_first_date), str(last_quarter_last_date)

    elif arg_required_time_period == "LAST_YEAR":
        year = 2022
        first_day_of_year = date.min.replace(year=year)
        last_day_of_year = date.max.replace(year=year)
        start_day_of_prev_month = datetime.combine(first_day_of_year, datetime.min.time())
        last_day_of_prev_month = datetime.combine(last_day_of_year, datetime.max.time())
        start_day_of_prev_month = start_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        start_day_of_prev_month = int(float(start_day_of_prev_month.timestamp()))
        last_day_of_prev_month = last_day_of_prev_month.replace(tzinfo=timezone('GMT'))
        last_day_of_prev_month = int(float(last_day_of_prev_month.timestamp()))
        return 12, str(start_day_of_prev_month), str(last_day_of_prev_month)

    elif arg_required_time_period == "LAST_SEVEN_DAYS":
        current_time = time.time()  # Get current epoch timestamp
        ## start date
        start_date = (datetime.fromtimestamp(current_time) - timedelta(days=7))
        start_date = datetime.combine(start_date.date(), datetime.min.time())
        start_date = start_date.replace(tzinfo=timezone('GMT'))
        print(start_date.strftime('%Y-%m-%d'))
        start_date = int(start_date.timestamp())
        ## end date
        end_date = (datetime.fromtimestamp(current_time) - timedelta(days=1))
        end_date = datetime.combine(end_date.date(), datetime.max.time())
        end_date = end_date.replace(tzinfo=timezone('GMT'))
        print(end_date.strftime('%Y-%m-%d'))
        end_date = int(end_date.timestamp())
        return 1, str(start_date), str(end_date)

    elif arg_required_time_period=="LAST_WEEK":
        current_datetime = datetime.now()
        # Calculate the start date and time of the last week
        start_of_last_week = current_datetime - timedelta(days=current_datetime.weekday() + 7)
        start_of_last_week = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_last_week = start_of_last_week.replace(tzinfo=timezone('GMT'))
        # Calculate the end date and time of the last week
        end_of_last_week = start_of_last_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        end_of_last_week = end_of_last_week.replace(tzinfo=timezone('GMT'))
        ## epoch coversion
        start_of_last_week = int(start_of_last_week.timestamp())
        end_of_last_week = int(end_of_last_week.timestamp())
        return 1, str(start_of_last_week), str(end_of_last_week)
        
    elif arg_required_time_period=="LAST_TWO_WEEKS":
        current_datetime = datetime.now()
        # Calculate the start date and time of the last week
        start_of_last_week = current_datetime - timedelta(days=current_datetime.weekday() + 14)
        start_of_last_week = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_last_week = start_of_last_week.replace(tzinfo=timezone('GMT'))
        # Calculate the end date and time of the last week
        end_of_last_week = start_of_last_week + timedelta(days=13, hours=23, minutes=59, seconds=59)
        end_of_last_week = end_of_last_week.replace(tzinfo=timezone('GMT'))
        ## epoch coversion
        start_of_last_week = int(start_of_last_week.timestamp())
        end_of_last_week = int(end_of_last_week.timestamp())
        return 1, str(start_of_last_week), str(end_of_last_week)
        
    elif arg_required_time_period=="LAST_FOURTEEN_DAYS":
        current_time = time.time()  # Get current epoch timestamp
        ## start date
        start_date = (datetime.fromtimestamp(current_time) - timedelta(days=14))
        start_date = datetime.combine(start_date.date(), datetime.min.time())
        start_date = start_date.replace(tzinfo=timezone('GMT'))
        print(start_date.strftime('%Y-%m-%d'))
        start_date = int(start_date.timestamp())
        ## end date
        end_date = (datetime.fromtimestamp(current_time) - timedelta(days=1))
        end_date = datetime.combine(end_date.date(), datetime.max.time())
        end_date = end_date.replace(tzinfo=timezone('GMT'))
        print(end_date.strftime('%Y-%m-%d'))
        end_date = int(end_date.timestamp())
        return 1, str(start_date), str(end_date)

    elif arg_required_time_period == "LAST_THIRTY_DAYS":
        current_time = time.time()  # Get current epoch timestamp
        ## start date
        start_date = (datetime.fromtimestamp(current_time) - timedelta(days=30))
        start_date = datetime.combine(start_date.date(), datetime.min.time())
        start_date = start_date.replace(tzinfo=timezone('GMT'))
        print(start_date.strftime('%Y-%m-%d'))
        start_date = int(start_date.timestamp())
        ## end date
        end_date = (datetime.fromtimestamp(current_time) - timedelta(days=1))
        end_date = datetime.combine(end_date.date(), datetime.max.time())
        end_date = end_date.replace(tzinfo=timezone('GMT'))
        print(end_date.strftime('%Y-%m-%d'))
        end_date = int(end_date.timestamp())
        return 1, str(start_date), str(end_date)

    else:
        return "<Logic not yet implemented>"
