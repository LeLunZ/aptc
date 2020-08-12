from lxml import html

from Functions.oebb_requests import requests_retry_session

service_months = {
    'Jan': 1,
    'Feb': 2,
    'Mär': 3,
    'Apr': 4,
    'Mai': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Okt': 10,
    'Nov': 11,
    'Dez': 12
}


class OebbDate:
    def __init__(self):
        self.day = None
        self.month = None
        self.year = None
        self.extend = None

    def __int__(self):
        day = str(self.day)
        if len(day) == 1:
            day = f'0{self.day}'
        month = service_months[self.month]
        if month < 10:
            month = f'0{service_months[self.month]}'
        return int(f'{self.year}{month}{day}')

    def __str__(self):
        day = str(self.day)
        if len(day) == 1:
            day = f'0{self.day}'
        month = service_months[self.month]
        if month < 10:
            month = f'0{service_months[self.month]}'
        return f'{day}{month}{self.year}'


begin_date: OebbDate = OebbDate()
end_date: OebbDate = OebbDate()


def get_std_date():
    global begin_date, end_date
    website = requests_retry_session(retries=10).get('http://fahrplan.oebb.at/bin/query.exe/dn?')
    tree = html.fromstring(website.content)
    validity = tree.xpath('//*/span[@class=$validity]/text()', validity='timetable_validity')[0]
    end = validity.split('bis')[1].replace('.', ' ').strip()
    begin = validity.split(' bis ')[0].split(' vom ')[1].replace('.', ' ').strip()
    inv_map = {v: k for k, v in service_months.items()}
    date_begin = begin.split(' ')
    begin_date.day = int(date_begin[0])
    begin_date.month = inv_map[int(date_begin[1])]
    begin_date.year = int(date_begin[2])
    date_end = end.split(' ')
    end_date.day = int(date_end[0])
    end_date.month = inv_map[int(date_end[1])]
    end_date.year = int(date_end[2])

