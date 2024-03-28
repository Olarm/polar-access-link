import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time


def login(email, password):
    url = "https://flow.polar.com/login"
    data = {"email": email, "password": password}
    session = requests.Session()
    response = session.post(url, data)
    return session


def create_url(today):
    #today = datetime.today()
    y = today.year
    m = today.month
    d = today.day
    u = int(time.mktime(today.timetuple()) * 1000)
    #u = int(time.time() * 1000)
    return f"https://flow.polar.com/activity/summary/{d}.{m}.{y}/{d}.{m}.{y}/day?_={u}"


def get_yesterday(email, password):
    session = login(email, password)
    yesterday = datetime.today() - timedelta(days=1)
    url = create_url(yesterday)
    r = session.get(url)
    data = parse_response(r.text)
    data["date"] = yesterday
    return data


def parse_response(r_text):
    soup = BeautifulSoup(r_text, "html.parser")
    spans = soup.findAll("span")
    keys = [
        'active time tracked', 
        'steps counted',
        'km measured in steps',
        'kilocalories burned',
        'inactivity stamps',
        'Sleep time'
    ]

    ret = {}

    for i, span in enumerate(spans):
        text = span.get_text()
        if text in keys:
            value = spans[i-1].get_text()
            delta = None
            if text == "Sleep time" or text == "active time tracked":
                t = datetime.strptime(value, "%H hours %M minutes")
                delta = timedelta(hours=t.hour, minutes=t.minute)
                ret[text] = delta.seconds
            else:
                ret[text] = value

    return ret