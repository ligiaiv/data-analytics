#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This module contains functions for handling dates,
creating timelines, counting occurrences by interval,
calculating elapsed timea d setting time zones.
'''

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import groupby
from time import time, sleep

def count_by_date(dict_by_date, str_date, str_var):
    '''
    Adds a date to the dates dictionary and the
    usernames or strings related to this date.
    '''
    try: dict_by_date[str_date].add(str_var)
    except KeyError: dict_by_date[str_date] = set([str_var])

def create_time_steps(timestamps_list):
    '''
    Create time intervals.
    '''
    time_step = min(timestamps_list)
    time_step = time_step.strftime('%d/%m/%Y')
    time_step = datetime.strptime(time_step, '%d/%m/%Y')
    delta = timedelta(1)
    step_number = 0
    time_intervals = []

    while time_step < max(timestamps_list):
        time_intervals.append((time_step, step_number))
        time_step = time_step + delta
        step_number += 1
    return sorted(time_intervals, key = lambda x: x[1]) #[(datetime, day_number), ...]

def datetime_from_str(str_date, str_format='%d/%m/%Y'):
    '''
    Convert string in DD/MM/YYYY format to datetime object,
    eg. "16/02/2016 17:38:53" to datetime(2016, 2, 16, 17, 38, 53).
    '''
    return datetime.strptime(str_date, str_format)

def datetime_from_timestamp(timestamp, tz=None, utc=False):
    '''
    Convert timestamp to datetime object.
    '''
    timestamp = int(timestamp + tz) if isinstance(tz, int) else timestamp
    if tz == 0 or utc: return datetime.utcfromtimestamp(timestamp)
    else: return datetime.fromtimestamp(timestamp)

def datetime_to_str(datetime, str_format='%d/%m/%Y'):
    '''
    Convert datetime object to string in DD/MM/YYYY format,
    eg. datetime(2016, 2, 16, 17, 38, 53) to "16/02/2016 17:38:53".
    '''
    return datetime.strftime(str_format)

def datetime_to_timestamp(datetime, utc=True):
    '''
    Convert datetime object to timestamp,
    eg. datetime(2016, 2, 16, 17, 38, 53) to 1455651533.
    '''
    if utc: return int(datetime.replace(tzinfo=timezone.utc).timestamp())
    else: return int(datetime.timestamp())

def get_local_tz():
    '''
    Calculate difference between local time and UTC.
    '''
    time_now = int(time())
    time_local = datetime_from_timestamp(time_now).timestamp()
    time_utc = datetime_from_timestamp(time_now, utc=True).timestamp()
    return (time_local-time_utc)

def get_time_diff(max_date, min_date):
    '''
    Return seconds elapsed during set period.
    '''
    seconds = int((max_date - min_date).total_seconds())
    time_string = ('hours' if seconds <= (24*3600) else 'days')
    time_string = ('minutes' if seconds <= 3600 else time_string)
    time_string = ('seconds' if seconds <= 60 else time_string)
    time_diff = 3600 if seconds <= (24*3600) else (24*3600)
    time_diff = 60 if seconds <= 3600 else time_diff
    time_diff = 1 if seconds <= 60 else time_diff
    return (seconds/time_diff), time_string, seconds

def get_time_elapsed(start_time):
    '''
    Return time elapsed since start time.
    '''
    end_time = int(time())
    time_elapsed = end_time - start_time
    time_elapsed = str(timedelta(seconds=time_elapsed))
    return time_elapsed

def set_time_zone(seconds=None):
    '''
    Set time zone from given seconds or local time.
    '''
    try: seconds = int(seconds)*3600 # see if seconds were given
    except: is_local_time = True # get current time zone from machine
    else: is_local_time = False # set time zone from seconds
    seconds = get_local_tz() if is_local_time else seconds
    tz = 'UTC' if seconds == 0 else str(int(seconds/60/60))
    tz = ('+' + tz) if seconds > 0 else tz
    tz =  tz + (' local' if is_local_time else '')
    print(tz, 'time zone set.')
    return seconds

def sleep_seconds(tts):
    '''
    Sleep for a given amount of seconds.
    '''
    ttw = datetime.fromtimestamp(int(time() + tts))
    ttw = datetime.strftime(ttw, "%H:%M:%S")
    print('\nSleeping', str(int(tts)) + 's until', ttw + '.')
    for i in range(3):
        sleep(0.5)
        print('.')
    sleep(tts)

def time_period_grouper(start_date, some_date):
    '''
    Function used to group by day.
    '''
    return(some_date-start_date).days // 1

def word_over_time(timestamps_list):
    '''
    Get words per given period.
    '''
    startdate = min(timestamps_list)
    rounded_startdate = startdate.strftime('%d/%m/%Y')
    rounded_startdate = datetime.strptime(rounded_startdate, '%d/%m/%Y')
    timestamps_list.sort()
    word_per_day = {}

    for day, number_of_dates in groupby(timestamps_list, key=lambda x:time_period_grouper(rounded_startdate, x)):
        word_per_day[day] = len(list(number_of_dates))

    return word_per_day

def comments_per_day(list_datetime_commments):
    '''
    Creates the comments per day timeline.
    '''
    list_str_date = fill_days_list(list_datetime_commments)
    dict_int_str_date = defaultdict(int)

    for str_day in list_str_date:
        dict_int_str_date[str_day] += 0

    for datetime in list_datetime_commments:
        str_date = datetime_to_str_date(datetime)
        dict_int_str_date[str_date] += 1

    return dict_int_str_date

def fill_days_list(datetimes_list):
    '''
    Receives a list of timestamp and dreturns a list of
    the returned list is in the format DD/MM/YYYY.
    '''
    max_date = max(datetimes_list)
    delta = timedelta(1) # one day delta
    complete_dates_list = []
    temp_date = min(datetimes_list)

    while temp_date < max_date:
        complete_dates_list.append(temp_date)
        temp_date = temp_date + delta
    return [datetime_to_str_date(x) for x in complete_dates_list]

def normalize_posts_by_date(dict_int_dates, time_string):
    '''
    Normalize posts by date by adding the missing days in a range of days, eg.:
    > If a list of dates has 2 posts in 17/03/2013, then it skips to 5 posts in 19/03/2013.
    The 18/03/2013 data point wouldn't exist, this function fills the empty days with zero.
    '''
    list_str_dates = dict_int_dates.keys()
    list_str_timestamps = []

    for str_date in list_str_dates:
        timestamp = datetime.strptime(str_date, time_string)
        list_str_timestamps.append(timestamp)

    max_date = max(list_str_timestamps)
    time_step = min(list_str_timestamps)
    delta = timedelta(1)

    while time_step < max_date:
        str_normal_date = time_step.strftime(time_string)
        if str_normal_date in list_str_dates:
            pass
        else:
            dict_int_dates[str_normal_date] = 0
        time_step = time_step + delta