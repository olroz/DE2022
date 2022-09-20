"""
Scrape web stores
"""
import os
import re
import requests


API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


def get_sales(date: str):
    """
    Scrape web stores
    """

    fulllist = []
    status_code = 501
    if bool(re.match(r"[0-9+]+-[0-9+]+-[0-9]", date)):

        i = 1
        response = requests.get(
            url=API_URL + "sales?date=" + date + "&page=" + str(i),
            headers={'Authorization':AUTH_TOKEN},
            timeout=5
        )
        #print(response)
        status_code = response.status_code

        while response.status_code == 200:
            print(str(i))
            fulllist = fulllist + response.json()
            i+=1
            response = requests.get(
                url=API_URL + "sales?date=" + date + "&page=" + str(i),
                headers={'Authorization':AUTH_TOKEN},
                timeout=5
            )

    if fulllist:
        return {
                "data": fulllist,
                }, status_code
    else:
        return {
                "data": '',
                }, status_code
