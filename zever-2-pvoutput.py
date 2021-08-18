import sys
import csv
import rich
import time
import logging
import datetime
import requests
import argparse

from config import *
from pytz import timezone
from pathlib import Path
from rich.console import Console
from astral.sun import sun
from astral import LocationInfo
from rich.logging import RichHandler
from requests.models import HTTPError
from astral.geocoder import lookup, database
from requests.exceptions import Timeout, ConnectionError, RetryError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Check is sun is set
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
                        'status': data[12],
                        'PAC_W': int(data[10]),
                        'E_TODAY': correct_E_Today(data[11])[0]}

    return formatted_data


def log_inverter_data(data):
    
    # Print data to console
    for key, value in data.items():
            console.print(f'[bold]{key} : [blue]{value}[/]')
            if key == 'status' and value == 'Error':
                logging.warning('Status Error')
    
    try:
        with open(db_path, 'a') as db:
            writer = csv.DictWriter(db, fieldnames=fieldnames)
            writer.writerow(data)
    except Exception as e:
        logging.warning(e)
        logging.warning('Error logging to local database')
    else:
        logging.info('Data logged to local database')



# Init requests session
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

    # Parse commandline args
    parser = argparse.ArgumentParser(description='Retreive inverter data and store in CSV file.')
    parser.add_argument('-ip', type=str, help='IP address of inverter')
    parser.add_argument('-interval', type=int, help='Data request interval in seconds')
    arg = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler()])
    console = Console()

    # Initialise location info
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

    # # Check for API info
    # if 'API_KEY' or 'SYSTEM_ID' not in globals():
    #     logging.critical('PVOutput API credentials not found')
    #     sys.exit(1)

    # Set inverter IP
    if arg.ip:
        ip = arg.ip
    elif 'INVERTER_IP' in globals():
        ip = globals()['INVERTER_IP']
    else:
        logging.critical('Inverter IP address is not set')
        sys.exit(1)

    inverter_url = f'http://{ip}/home.cgi'


     # Init PVOutput, inverter requests sessions
    pvoutput_session = requests_retry_session()
    pvoutput_session.headers.update({ 
        'X-Pvoutput-Apikey': API_KEY,
        'X-Pvoutput-SystemId': SYSTEM_ID})
    
    inverter_session = requests_retry_session()


    # Set data request interval, system name
    try:
        response = pvoutput_session.get('https://pvoutput.org/service/r2/getsystem.jsp', timeout=5)
        response.raise_for_status()
    except HTTPError as e:
        if response.reason == 'Unauthorized':
            logging.warning('Could not authenticate with PVOutput API - Check API settings')
            sys.exit(1)
    except Exception as e:
        logging.warning(e)
        logging.warning(f'Error retrieving data interval from PVOutput - Default of {DEFAULT_REQ_INTERVAL} second(s) will be used')
        request_interval = DEFAULT_REQ_INTERVAL
    else:
        data = response.text.split(',')
        if arg.interval:
            request_interval = arg.interval
        else:
            request_interval = int(data[15][:-3]) * 60
        system_name = data[0]
    
    
    
    
    # Create database at DB_DIR if one does not exist
    fieldnames = ['date', 'time', 'status', 'PAC_W', 'E_TODAY']
    db_name = f'{system_name} database.csv'
    db_path = Path(DB_DIR) / db_name
    if db_path.exists() == False:
        try:
            with open(db_path, 'x') as db:
                writer = csv.DictWriter(db, fieldnames=fieldnames)
                writer.writeheader()
        except Exception as e:
            logging.info(e)
        else:
            logging.info(f'New database created at {db_path}')
    else:
        logging.info(f'Logging to existing database at {db_path}')
        
        
    # Main loop
    while True:
        
        print(f'-----------------------------------------')
        print(f'Collecting data for {system_name}')
        print(f'-----------------------------------------')
        
        while daylight_hours():
            # Grab data from inverter
            try:
                logging.info('Grabbing data from inverter')
                response = inverter_session.get(inverter_url, timeout=5)
                response.raise_for_status()
            except HTTPError as e:
                logging.warning('HTTP error')
                logging.warning(e)
            except ConnectionError as e:
                logging.warning('Connection Error')
            except (RetryError, Timeout) as e:
                logging.warning('Connection timed out')
            except Exception as e:
                print('Some other error')
                print(e)
            else:
                inverter_data = parse_inverter_data(response.text)
                log_inverter_data(inverter_data)

                # Upload data to PVOutput
                payload = {    
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
            with console.status('Sun has set - Resumimg at sunrise'):
                while daylight_hours() == False:
                    time.sleep(request_interval)
