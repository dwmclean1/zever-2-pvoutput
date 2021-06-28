import sys
import csv
import time
import logging
import datetime
import requests
import argparse

from config import *
from pytz import timezone
from rich.console import Console
from astral.sun import sun
from astral import LocationInfo
from rich.logging import RichHandler
from requests.models import HTTPError
from astral.geocoder import lookup, database
from requests.exceptions import Timeout, ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# Check if sun is set
def daylight_hours():
    
    s = sun(location.observer, date=datetime.date.today(), tzinfo=location.timezone)
    current_time = datetime.datetime.now(tz=loc_tz)
    sunrise = s['sunrise']
    sunset = s['sunset']

    if current_time > sunrise and current_time < sunset:
        return True
    else:
        return False


# Insert missing leading zero from energy reading
def correct_E_Today(kwh):
    
    parts = kwh.split('.')
    if int(parts[1]) < 10:
        kwh = f'{parts[0]}.0{parts[1]}'

    watts = float(kwh) * 1000
    return int(watts), float(kwh)


# Parse data from inverter
def parse_inverter_data(data):
    data = data.split('\n')
    date = datetime.date.today().isoformat()
    date = date.replace('-', '')
    t = datetime.datetime.now().strftime('%H:%M')

    formatted_data = {  'date': date,
                        'time': t,
                        'status': data[7],
                        'PAC_W': int(data[10]),
                        'E_TODAY': correct_E_Today(data[11])[0]}

    return formatted_data


# Database logging
def log_inverter_data(data):

    fieldnames = ['date', 'time', 'status', 'PAC_W', 'E_TODAY']

    for key, value in data.items():
            console.print(f'[bold]{key} : [blue]{value}[/]')

    try:
        with open('database.csv', 'x') as db:
            writer = csv.DictWriter(db, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(data)
    except FileExistsError:
        with open('database.csv', 'a') as db:
            writer = csv.DictWriter(db, fieldnames=fieldnames)
            writer.writerow(data)
    except Exception as e:
        logging.warning(e)
        logging.warning('Error logging to local database')
    finally:
        logging.info('Data logged to local database')


# Initalise requests session
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=[500, 502, 504],
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


if __name__ == "__main__":

    # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Parse commandline arguments ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
    parser = argparse.ArgumentParser(description='Retreive inverter data and store in CSV file.')
    parser.add_argument('-ip', type=str, help='IP address of inverter')
    parser.add_argument('-interval', type=int, help='Data request interval in seconds')
    arg = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler()])
    console = Console()

    # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Load settings in config.py ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

    # Location
    try: 
        geo_info = lookup(CITY, database())
    except Exception as e:
        logging.critical(e)
        sys.exit(1)

    location = LocationInfo()
    location.name = geo_info.name
    location.region = geo_info.region
    location.timezone = geo_info.timezone
    location.latitude = geo_info.latitude
    location.longitude = geo_info.longitude
    loc_tz = timezone(location.timezone)

    # API credentials
    # if 'API_KEY' or 'SYSTEM_ID' not in globals():
    #     logging.critical('PVOutput API credentials not found')
    #     sys.exit(1)

    # Inverter IP
    if arg.ip:
        ip = arg.ip
    elif 'INVERTER_IP' in globals():
        ip = globals()['INVERTER_IP']
    else:
        logging.critical('Inverter IP address is not set')
        sys.exit(1)

    inverter_url = f'http://{ip}/home.cgi'


    # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Initalise PVOutput requests session ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
    pvoutput_session = requests_retry_session()
    pvoutput_session.headers.update({ 
        'X-Pvoutput-Apikey': API_KEY,
        'X-Pvoutput-SystemId': SYSTEM_ID})


    # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Set data request interval, system name ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
    if arg.interval:
        request_interval = arg.interval
    else:
        try:
            response = pvoutput_session.get('https://pvoutput.org/service/r2/getsystem.jsp', timeout=5)
        except Exception as e:
            logging.warning(e)
            logging.warning(f'Error retreiving data interval from PVOutput - Default of {DEFAULT_REQ_INTERVAL} second(s) will be used')
            request_interval = DEFAULT_REQ_INTERVAL
        else:
            data = response.text.split(',')
            request_interval = int(data[15][:-3]) * 60
            system_name = data[0]
    
    print(f'-----------------------------------------')
    print(f'Collecting data for {system_name}')
    print(f'-----------------------------------------')

    # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Main loop ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
    while daylight_hours():

        # Grab data from inverter
        inverter_session = requests_retry_session()
        try:
            logging.info('Grabbing data from inverter')
            response = inverter_session.get(inverter_url, timeout=5)
            response.raise_for_status()
        except HTTPError as e:
            logging.warning('HTTP error')
            logging.warning(e)
        except ConnectionError as e:
            logging.warning('Connection Error')
        except Timeout as e:
            logging.warningl('Connection timed out')
        else:
            inverter_data = parse_inverter_data(response.text)
            log_inverter_data(inverter_data)

            # ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Upload data to PVOutput ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
            payload = {    
                'c1': 2,                               # Cumulative Flag
                'd': inverter_data['date'],            # Date
                't': inverter_data['time'],            # Time
                'v1': inverter_data['E_TODAY'],        # Energy Generation
                'v2': inverter_data['PAC_W']           # Power Generation
                }

            try:
                response = pvoutput_session.post('https://pvoutput.org/service/r2/addstatus.jsp', data=payload)
                response.raise_for_status()
            except Exception as e:
                logging.warning(e)
            else:
                logging.info('Data uploaded to PVoutput')
                
        with console.status(f'Retrying in {int(request_interval / 60)} minutes', spinner='dots12'):
            time.sleep(request_interval)

    else:
        logging.info('Sun has set - Resumimg at sunrise', end='\r')
        time.sleep(request_interval)