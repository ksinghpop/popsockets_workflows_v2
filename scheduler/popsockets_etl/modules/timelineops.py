from dateutil.parser import parse
import datetime as dt
from datetime import timedelta, timezone

class ValidateTimelineUTC:
    current_datetime_utc = dt.datetime.now(timezone.utc)
    current_date_utc = current_datetime_utc.date()

    def convert_str_to_dt_utc(date:str):
        parsed_date = parse(date)
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
        else:
            parsed_date = parsed_date.astimezone(timezone.utc)
        
        return parsed_date

    def last_n_minutes(minutes,date:str):
        date_obj = ValidateTimelineUTC.convert_str_to_dt_utc(date)
        target_datetime = ValidateTimelineUTC.current_datetime_utc-timedelta(minutes=minutes)
        if date_obj >= target_datetime:
            return True
        else:
            return False

    def last_n_hours(hours:int,date:str):
        date_obj = ValidateTimelineUTC.convert_str_to_dt_utc(date)
        target_datetime = ValidateTimelineUTC.current_datetime_utc-timedelta(hours=hours)
        if date_obj >= target_datetime:
            return True
        else:
            return False
        
    def last_n_days(days,date:str):
        date_obj = ValidateTimelineUTC.convert_str_to_dt_utc(date)
        target_datetime = ValidateTimelineUTC.current_datetime_utc-timedelta(days=days)
        if date_obj >= target_datetime:
            return True
        else:
            return False
        
    def this_day(date:str):
        date_obj = ValidateTimelineUTC.convert_str_to_dt_utc(date)
        if date_obj.date()==ValidateTimelineUTC.current_date_utc:
            return True
        else:
            return False
    
    def last_day(date:str):
        date_obj = ValidateTimelineUTC.convert_str_to_dt_utc(date)
        target_datetime = ValidateTimelineUTC.current_date_utc-timedelta(days=1)
        if date_obj.date()==target_datetime:
            return True
        else:
            return False