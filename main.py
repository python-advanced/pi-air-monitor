import os
import pymongo
from bson.json_util import dumps
from pprint import pprint
from numpy import mean
from playsound import playsound
from datetime import datetime, timedelta


client = pymongo.MongoClient() # add your connection string if not local 
change_stream = client.air.air.watch()

labels = ['VOC-CCS', 'VOC-TGS', 'PM25', 'PM10']


def min2sec(m):
    return m * 60


def check_wrapper(dict_data, checking_function):
    print('checking ', checking_function.__name__)
    reached = []
    for label in labels:
        if checking_function([d[label] for d in dict_data]):
            reached.append(label)

    return reached


def check_sudden_rise(data):
    avg_5min = mean(data[-min2sec(5):])
    for i in range(1, 11):  # 10 seconds
        if data[-i] < avg_5min * 1.3 and data[-i] > 3:  # increased 30%
            return False
    return True


def activate_warning(msg):
    print(msg)
    playsound('./warning.wav', block=False)
    # you can send message here


def construct_name(warning_name, labels):
    return warning_name + str(labels)


def check_timing(warnings, warning):
    if warning in warnings:
        diff = datetime.now() - warnings[warning]
        if diff < timedelta(minutes=5):
            print('filtered out warning due to time diff =', diff)
            return False

    warnings[warning] = datetime.now()
    return True


warnings = {}
data = []
for change in change_stream:
    d = change['fullDocument']
    # pprint(d)
    data.append(d)
    print(len(data))

    # start checking after 5 minutes so we have enough data
    if len(data) > min2sec(5):
        sudden_rise = check_wrapper(data, check_sudden_rise)
        if sudden_rise:
            warning = construct_name('sudden_rise', sudden_rise)
            if check_timing(warnings, warning):
                activate_warning(warning)

    # after 10 minutes, start popping data to avoid data size too large
    if len(data) > min2sec(10):
        data.pop(0)
