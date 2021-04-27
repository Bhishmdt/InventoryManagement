from random import randrange
from datetime import timedelta, datetime

def random_date(start = datetime.strptime('2018/1/1', '%Y/%m/%d'), end = datetime.strptime('2020/12/31', '%Y/%m/%d')):
    delta = end - start
    int_delta = delta.days
    random_day = randrange(int_delta)
    r = start + timedelta(days=random_day)
    r = r.strftime('%Y/%m/%d')
    return r

if __name__ == '__main__':
    print(random_date())