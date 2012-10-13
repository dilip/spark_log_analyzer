import calendar

def dt2Epoch(dt):
    return calendar.timegm(dt.timetuple())
