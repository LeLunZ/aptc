import datetime

from Models.calendar import Calendar
from Classes.oebb_date import end_date, OebbDate, service_months

inv_map = {v: k for k, v in service_months.items()}


def get_all_name_of_transport_distinct(list_of_transport):
    all_transport = set()
    for transport in list_of_transport:
        if transport['pr'] not in all_transport:
            all_transport.add(transport['pr'])
    return list(all_transport)


def remove_param_from_url(url, to_remove):
    splitted_url = url.split(to_remove)
    left_part = splitted_url[0]
    right_part = '&'.join(list(filter(lambda x: x.strip() != '', splitted_url[1].split('&')[::-1][:-1])))
    return left_part + '&' + right_part


def extract_date_from_date_arr(date_arr: [str]):
    date = OebbDate()
    date.day = int(date_arr[0].replace('.', ''))
    date.month = None
    date.year = None
    if len(date_arr) > 1 and date_arr[1] in service_months:
        date.month = date_arr[1]
        try:
            date.year = int(date_arr[2])
        except:
            pass
    return date


def merge_date(date1: OebbDate, date2: OebbDate):
    if date1.month is None:
        date1.month = date2.month
    if date1.year is None and date2.year is None:
        date1.year = end_date.year
        date2.year = end_date.year
    if date1.year is None and date2.year is not None:
        date1.year = date2.year


def add_day_to_calendar(calendar: Calendar, week_day: str):
    if week_day == 'Mo':
        calendar.monday = True
    if week_day == 'Di':
        calendar.tuesday = True
    if week_day == 'Mi':
        calendar.wednesday = True
    if week_day == 'Do':
        calendar.thursday = True
    if week_day == 'Fr':
        calendar.friday = True
    if week_day == 'Sa':
        calendar.saturday = True
    if week_day == 'So':
        calendar.sunday = True


def add_days_to_calendar(calendar: Calendar, begin: str, end: str):
    official_weekdays = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    for d in official_weekdays[official_weekdays.index(begin):official_weekdays.index(end) + 1]:
        add_day_to_calendar(calendar, d)


def str_to_geocord(cord: str):
    return int(cord) / 1000000


def skip_stop(seq, begin, end):
    for i, item in enumerate(seq):
        if begin <= i <= end and item[-1] and item[-1] != '':
            yield item


def extract_date_objects_from_str(dates_list, dates, start_date, finish_date):
    date_arr = dates.replace('.', '').replace(',', '').split(' ')
    while 'bis' in date_arr:
        index = date_arr.index('bis')
        possible_1 = date_arr[index - 1].replace('.', '')
        first_index = None
        last_index = None
        date = None
        date2 = None
        try:
            day = int(possible_1) if 0 < int(possible_1) < 32 else None
            year = int(possible_1) if day is None else None
            if year is None:
                date = extract_date_from_date_arr(date_arr[index - 1:index])
                first_index = index - 1
            else:
                date = extract_date_from_date_arr(date_arr[index - 3:index])
                first_index = index - 3
        except:
            month = possible_1
            if month in service_months:
                date = extract_date_from_date_arr(date_arr[index - 2:index])
                first_index = index - 2
        if date is not None:
            possible_1 = date_arr[index + 1].replace('.', '')
            try:
                day = int(possible_1) if 0 < int(possible_1) < 32 else None  # leave it here. for exception
                date2 = extract_date_from_date_arr(date_arr[index + 1:index + 4])
                last_index = index + 4
            except:
                date2 = None
        if date is not None and date2 is not None:
            merge_date(date, date2)
            newDate = None
            while not (date.day == date2.day and date.month == date2.month and date.year == date2.year):
                try:
                    newDate = OebbDate()
                    newDate.day = date.day
                    newDate.month = date.month
                    newDate.year = date.year
                    newDate.extend = False
                    dates_list.append(newDate)
                    datetime.datetime(date.year, service_months[date.month], date.day + 1)
                    date.day = date.day + 1
                except:
                    try:
                        datetime.datetime(date.year, service_months[date.month] + 1, 1)
                        date.month = inv_map[service_months[date.month] + 1]
                        date.day = 1
                    except:
                        try:
                            datetime.datetime(date.year + 1, 1, 1)
                            date.year = date.year + 1
                            date.month = 'Jan'
                            date.day = 1
                        except:
                            pass
            if newDate is not None:
                newDate = OebbDate()
                newDate.day = date.day
                newDate.month = date.month
                newDate.year = date.year
                newDate.extend = False
                dates_list.append(newDate)
        for i in range(first_index, last_index):
            date_arr.pop(first_index)
    date_arr.reverse()
    year = None
    month = None
    for count, d in enumerate(date_arr):
        try:
            test_day = int(d)
            if test_day > 31:
                raise Exception()
            if year is not None and month is not None:
                try:
                    day = int(d)
                    date = OebbDate()
                    date.year = year
                    date.month = month
                    date.day = day
                    date.extend = False
                    dates_list.append(date)
                    continue
                except:
                    pass
            if year is None and month is not None:
                try:
                    day = int(d)
                    date = OebbDate()
                    if service_months[month] > service_months[finish_date.month] or (
                            service_months[month] is service_months[finish_date.month] and day > finish_date.day):
                        date.year = start_date.year
                    else:
                        date.year = finish_date.year
                    date.month = month
                    date.day = day
                    date.extend = False
                    dates_list.append(date)
                    continue
                except:
                    pass
        except:
            pass
        try:
            year = int(d)
            if year < finish_date.year or year > finish_date.year:
                raise Exception()
            continue
        except:
            year = None
        if d in service_months:
            month = d
            continue
        else:
            month = None
