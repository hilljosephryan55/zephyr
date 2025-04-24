import numpy as np
from fastapi import FastAPI, Form, Request, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.responses import StreamingResponse
from http.client import IncompleteRead, RemoteDisconnected
import urllib
from datetime import timedelta
import time
import logging
import pandas as pd
import os
import s3fs
import boto3



# Directory containing this script (frontend.py) -> C:\Users\hillj\repos\zephyr\zephyr
SCRIPT_DIRECTORY = os.path.dirname(__file__)

# Project Root Directory (one level up) -> C:\Users\hillj\repos\zephyr
PROJECT_ROOT_DIRECTORY = os.path.dirname(SCRIPT_DIRECTORY)

# Path to the 'templates' directory -> C:\Users\hillj\repos\zephyr\templates
TEMPLATE_PATH = os.path.join(PROJECT_ROOT_DIRECTORY, "templates")

# Path to the 'static' directory -> C:\Users\hillj\repos\zephyr\static
STATIC_FILES_PATH = os.path.join(PROJECT_ROOT_DIRECTORY, "static")


# --- App Creation and Configuration ---

# Initialize FastAPI
app = FastAPI()

# Initialize Jinja2Templates
templates = Jinja2Templates(directory=TEMPLATE_PATH)

# Mount the static files directory
app.mount(
    "/static",
    StaticFiles(directory=STATIC_FILES_PATH),
    name="static",
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')




@app.get("/")
def read_root(request: Request, msg=""):
    """
    Generate temperature/dewpoint forecast tables and highcharts for actuals and forecast data.
    """

    ercot_cities = ['KBRO', 'KDFW', 'KAUS', 'KSAT', 'KIAH']


    # What be tomorrow?
    day_ahead = (pd.Timestamp.utcnow().tz_convert('US/Central') + pd.Timedelta(days=1)).normalize()

    # What be yesterday?
    day_behind = (pd.Timestamp.utcnow().tz_convert('US/Central') + pd.Timedelta(days=-1)).normalize()

    # What be today?
    today_accurate = (pd.Timestamp.utcnow().tz_convert('US/Central')).strftime('%Y-%m-%d %H:%M')
    today_short = pd.Timestamp.utcnow().tz_convert('US/Central').strftime('%Y-%m-%d')

    ercot_iadata = _city_temp_api_pull(ercot_cities, tz = 'central')

    # use the get_temperature_data() function to pull yesterday's wsi ercot data
    ercot_prev = get_temperature_data()
    ercot_prev = ercot_prev.drop('run_time', axis = 1)
    ercot_prev.index.names = ['Local Time']


    # select the ercot values corresponding to the current day
    prevalues = ercot_prev[24:48]

    # create a dataframe of just the temperatures or dewpoints
    combined_cities = ercot_cities
    tempdrop = []
    dpdrop = []
    for city in combined_cities:
        tempdrop.append(f'{city}_dewpoint')
        dpdrop.append(f'{city}_temp')

    tempvalues = prevalues.drop(tempdrop, axis = 1)
    dpvalues = prevalues.drop(dpdrop, axis = 1)

    # rename all the columns

    for city in combined_cities:
        tempvalues = tempvalues.rename(columns={f'{city}_temp': f'{city}_t_F'})
        dpvalues = dpvalues.rename(columns={f'{city}_dewpoint': f'{city}_dp_F'})


    new_columns = [name[1:] for name in tempvalues]
    tempvalues.columns = new_columns


    new_columns = [name[1:] for name in dpvalues]
    dpvalues.columns = new_columns

    # set a list of cities (minus the K at the beginning)
    cities_list = []
    for city in combined_cities:
        if (len(city) > 3) and city[0] == 'K' or city[0] == 'P':
            cities_list.append(city[1:])
        else:
            raise ValueError('One of the requested cities does not conform to the required K### format')


    iadata = pd.DataFrame()
    iadata = pd.concat([iadata, ercot_iadata])


    # create an empty dataframe to hold the iacity data
    ia_cities_df = pd.DataFrame()

    # for each city in the list above
    for city in cities_list:
        #create an empty list to house the temperature and dewpoint values
        temporary_temp_list = []
        temporary_dp_list = []
        # query the temperature and dewpoint data for the specified city
        iadata_temp = (iadata.query(f'station == "{city}"')['temp'])
        iadata_dp = (iadata.query(f'station == "{city}"')['dewpoint'])

        iadata_temp = (pd.to_numeric(iadata_temp[~iadata_temp.index.duplicated(keep='last') | ~iadata_temp.index.duplicated(keep='last')], errors = 'coerce')).sort_index()
        iadata_dp = (pd.to_numeric(iadata_dp[~iadata_dp.index.duplicated(keep='last') | ~iadata_dp.index.duplicated(keep='last')], errors = 'coerce')).sort_index()

        # for each hourly timestamp
        for timestep in tempvalues.index:
            # locate the index of the closest timestamp in the iadata to the wsi forecast data timestamp
            iloc_idx = iadata_temp.index.get_indexer([timestep], method='nearest')
            # grab the timestamps of iastate data and wsi forecast data for this timestep
            ia_time = iadata_temp.index[int(iloc_idx)].tz_localize(None)
            wsi_time = timestep.tz_localize(None)

            # if there is a difference of greater than 1 hour between the two, append a nan value for the actual values
            if wsi_time - ia_time > pd.Timedelta(1, "h"):
                temporary_temp_list.append(np.nan)
                temporary_dp_list.append(np.nan)
            # else append the actual values
            else:
                temporary_temp_list.append(iadata_temp[int(iloc_idx)])
                temporary_dp_list.append(iadata_dp[int(iloc_idx)])

        # attach the temp/dp lists to the total dataframe and move to the next city
        ia_cities_df[f'{city}_temp_A'] = temporary_temp_list
        ia_cities_df[f'{city}_dp_A'] = temporary_dp_list

    # insert the actual values into the previous dataframes at specific points

    # create an odd range list based on the number of cities
    odd_list = range(1, len(cities_list)*2, 2)

    # insert the actuals data into the overall data dataframes based on city
    for i in range(len(cities_list)):
        temp_column_name = f'{cities_list[i]}_t_A'
        temp_values = f'{cities_list[i]}_temp_A'
        dp_column_name = f'{cities_list[i]}_dp_A'
        dp_values = f'{cities_list[i]}_dp_A'

        tempvalues.insert(loc = odd_list[i], column = temp_column_name, value = ia_cities_df[temp_values].values)
        dpvalues.insert(loc=odd_list[i], column=dp_column_name, value=ia_cities_df[dp_values].values)

    # edit the indexes to just be the hour value
    tempvalues.index = tempvalues.index.strftime('%H')
    tempvalues = tempvalues.T

    dpvalues.index = dpvalues.index.strftime('%H')
    dpvalues = dpvalues.T

    # create the multi-index and assign it
    loc = []
    for location in cities_list:
        loc.extend([location,location])

    label_set = ['Fcst', 'Act']
    labels = label_set*len(cities_list)
    new_index = pd.MultiIndex.from_tuples(tuple(zip(loc, labels)))

    tempvalues.index = new_index
    dpvalues.index = new_index

    # round values
    tempvalues = tempvalues.round()
    dpvalues = dpvalues.round()

    ercot_cities = [name[1:] for name in ercot_cities]
    ercot_indexes = [i for i, x in enumerate(tempvalues.index.get_level_values(0)) if
                     x in ercot_cities]


    ercot_tempvalues = tempvalues.iloc[ercot_indexes]
    ercot_dpvalues = dpvalues.iloc[ercot_indexes]


    # use the helper function to create the temperature and dewpoint html tables
    # also return the numerical data for insertion into highcharts via context
    ercot_temperature_html, ercot_tempdata = _verification_table_generator(ercot_tempvalues,
                                                                           ercot_cities,
                                                                           new_index[ercot_indexes])
    ercot_dewpoint_html, ercot_dpdata = _verification_table_generator(ercot_dpvalues,
                                                                      ercot_cities,
                                                                      new_index[ercot_indexes])


    context = {'request': request,
               'msg': msg,
               'ercot_temperature_html': ercot_temperature_html,
               'ercot_dewpoint_html': ercot_dewpoint_html,

               'dfw_temp_f_data': list(ercot_tempdata.loc['DFW'].loc['Fcst']),
               'dfw_temp_a_data': list(ercot_tempdata.loc['DFW'].loc['Act']),
               'aus_temp_f_data': list(ercot_tempdata.loc['AUS'].loc['Fcst']),
               'aus_temp_a_data': list(ercot_tempdata.loc['AUS'].loc['Act']),
               'sat_temp_f_data': list(ercot_tempdata.loc['SAT'].loc['Fcst']),
               'sat_temp_a_data': list(ercot_tempdata.loc['SAT'].loc['Act']),
               'iah_temp_f_data': list(ercot_tempdata.loc['IAH'].loc['Fcst']),
               'iah_temp_a_data': list(ercot_tempdata.loc['IAH'].loc['Act']),
               'bro_temp_f_data': list(ercot_tempdata.loc['BRO'].loc['Fcst']),
               'bro_temp_a_data': list(ercot_tempdata.loc['BRO'].loc['Act']),
               'dfw_dp_f_data': list(ercot_dpdata.loc['DFW'].loc['Fcst']),
               'dfw_dp_a_data': list(ercot_dpdata.loc['DFW'].loc['Act']),
               'aus_dp_f_data': list(ercot_dpdata.loc['AUS'].loc['Fcst']),
               'aus_dp_a_data': list(ercot_dpdata.loc['AUS'].loc['Act']),
               'sat_dp_f_data': list(ercot_dpdata.loc['SAT'].loc['Fcst']),
               'sat_dp_a_data': list(ercot_dpdata.loc['SAT'].loc['Act']),
               'iah_dp_f_data': list(ercot_dpdata.loc['IAH'].loc['Fcst']),
               'iah_dp_a_data': list(ercot_dpdata.loc['IAH'].loc['Act']),
               'bro_dp_f_data': list(ercot_dpdata.loc['BRO'].loc['Fcst']),
               'bro_dp_a_data': list(ercot_dpdata.loc['BRO'].loc['Act']),


               'date_accurate': today_accurate,
               'date': today_short,
               'yesterday': day_behind.strftime('%Y-%m-%d')
               }
    return templates.TemplateResponse("index.html", context=context)




def _verification_table_generator(variablevalues, cities_list, new_index):
    """
    Takes a temperature or dewpoint forecast dataframe and turns it into an HTML table while calculating anomalies

    Parameters
    ----------
    variablevalues : 'Dataframe',
        a dataframe of hourly forecasted temperature values for the day
    cities_list : 'list'
        list of cities as strings
    new_index : 'multi-index'
        a multi-index containing the city and forecast/actual indicators

    Returns
    -------
    variable_html : 'html table'
        a forecast html table for a variable
    numbersdata : 'dataframe'
        a dataframe of temperature/dewpoint numerical values
    """
    # create an odd and even list for city indexing using the cities_list
    odd_list = range(1, len(cities_list)*2, 2)
    even_list = range(0, len(cities_list)*2, 2)

    # create an empty dictionary to house the city temperature/dewpoint anomaly data
    city_anom_data = {}

    # for each city, subtract the forecasted values FROM the actual values
    for id, city in enumerate(cities_list):
        city_anom_data[f'{city}'] = variablevalues.iloc[odd_list[id]] - variablevalues.iloc[even_list[id]]

    # create an all zeros array to place in the "forecast" anomaly difference locations
    all_zeros = np.zeros(24)

    # create an empty dataframe to house the anomaly numerical values (forecasted value anomalies should remain zero)
    anom_df = pd.DataFrame(columns = variablevalues.columns)

    for citycount, cityname in enumerate(cities_list):
        anom_df.loc[even_list[citycount]] = all_zeros
        anom_df.loc[even_list[citycount]+1] = city_anom_data[cityname]

    # create a copy of the anom_df and have it be a dataframe of string classes based on binned temperatures
    anom_class_df = anom_df.copy()
    anom_class_df.loc[:] = ""
    anom_class_df[anom_df <= -12] = "below15"
    anom_class_df[(anom_df > -12) & (anom_df <= -8)] = "below8"
    anom_class_df[(anom_df > -8) & (anom_df <= -5)] = "below5"
    anom_class_df[(anom_df > -5) & (anom_df <= -2)] = "below3"
    anom_class_df[(anom_df > -2) & (anom_df < 2)] = "normal"
    anom_class_df[(anom_df >= 2) & (anom_df < 5)] = "above3"
    anom_class_df[(anom_df >= 5) & (anom_df < 8)] = "above5"
    anom_class_df[(anom_df >= 8) & (anom_df < 12)] = "above8"
    anom_class_df[anom_df >= 12] = "above15"

    # attach the multi-index to the dataframe
    anom_class_df.index = new_index

    # replace all numbers data with 9999s and enforce it as an integer
    # the javascript code will find the 9999s and replace with its own null values
    numbersdata = variablevalues.copy()
    numbersdata = numbersdata.fillna(9999)
    numbersdata = numbersdata.astype('int')

    # replace the variable values with 9999s and then a blank string for html display
    variablevalues = ((variablevalues.replace(np.nan, 9999)).astype('int')).astype('str')
    variablevalues = variablevalues.replace('9999', '')

    # create the html table
    variable_html = ("<td class='" + anom_class_df + "'>" + variablevalues
                     ).to_html(escape=False, col_space = 35, classes="front_page", table_id="forecast").replace("<td>", "")

    return(variable_html, numbersdata)



def _city_temp_api_pull(cities, tz):
    """
    Polls the IAState API for the specified cities temperature and dewpoint data

    Parameters
    ----------
    cities : 'list'
        A list of cities identified by their airport code
    tz : 'string'
        The specified timezone the cities are in. Supports central, pacific, mountain, eastern

    Returns
    -------
    iadata : 'dataframe'
        A dataframe containing temperature/dewpoint values for the requested cities as a time series

    """
    # IASTATE Data Portion

    if tz.lower() == 'central':
        timezone = 'America%2FChicago'
    elif tz.lower() == 'pacific':
        timezone = 'America%2FLos_Angeles'
    elif tz.lower() == 'mountain':
        timezone = 'America%2FDenver'
    elif tz.lower() == 'eastern':
        timezone = 'America%2FNew_York'

    # What be tomorrow?
    day_ahead = (pd.Timestamp.utcnow().tz_convert('US/Central') + pd.Timedelta(days=1)).normalize()

    # What be yesterday?
    day_behind = (pd.Timestamp.utcnow().tz_convert('US/Central') + pd.Timedelta(days=-1)).normalize()

    # set the url pieces for the IASTATE api
    url_start = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?'
    url_end = 'data=all&year1=' + \
              str(day_behind.year) + \
              '&month1=' + \
              str(day_behind.month) + \
              '&day1=' + \
              str(day_behind.day) + \
              '&year2=' + \
              str(day_ahead.year) + \
              '&month2=' + \
              str(day_ahead.month) + \
              '&day2=' + \
              str(day_ahead.day) + \
              f'&tz={timezone}&format=onlycomma&latlon=yes&elev=yes&missing=M&trace=' \
              '0.0001&direct=yes&report_type=3&report_type=4'


    ### creat the middle part of the url where each of the stations are queried ###
    stations_for_url = []

    # create list of stations in the needed format
    for i in range(len(cities)):
        entry = 'station=' + cities[i] + '&'
        stations_for_url.append(entry)

    # join all the elements of the list
    stations_for_url = [''.join(stations_for_url)]

    # merge the whole url
    stationurl = url_start + stations_for_url[0] + url_end

    # attempt to snag the IASTATE data using the created API url
    # will attempt multiple times and wait an exponential amount of time between each try
    tries = 0
    max_tries = 8
    exponential_wait_time = 3
    while tries < max_tries:
        try:
            adf = pd.read_csv(stationurl)
            break
        except (urllib.error.URLError, IncompleteRead, RemoteDisconnected) as e:
            if tries < max_tries - 1:
                wait_time = exponential_wait_time ** tries
                logger.info(
                    f"Unsuccesful call to IA State API for data on try number {tries}; sleeping for {wait_time} seconds then trying again")
                time.sleep(wait_time)
                tries += 1
            else:
                logger.error(f"Error getting temp data from IA State API")
                raise e

    # create empty dataframe for the iadata and assign some values
    iadata = adf[['valid', 'station', 'tmpf', 'dwpf']].copy()
    iadata.rename(columns={'tmpf': 'temp', 'dwpf': 'dewpoint'}, inplace=True)
    iadata.index = pd.to_datetime(iadata['valid'])

    return(iadata)


def get_temperature_data(
    bucket_name: str = 'hill5599',
    s3_folder_prefix: str = 'weather-forecasts/temperatures/ercot/',
    filename_prefix: str = 'ercot_temps',
    filename_suffix: str = '.csv') -> pd.DataFrame | None:
    """
    Reads the previous day's temperature data CSV from S3 into a DataFrame.

    Determines the previous day based on current UTC time. Assumes the CSV
    files are named '{filename_prefix}_YYYY-MM-DD{filename_suffix}' and contain
    a 'LocalTime' column that can be parsed as datetime.

    Args:
        bucket_name (str): The S3 bucket name. Defaults to 'hill5599'.
        s3_folder_prefix (str): The folder path within the bucket. Defaults to
                                'weather-forecasts/temperatures/ercot/'.
                                Ensures it ends with '/'.
        filename_prefix (str): The prefix of the data files. Defaults to 'ercot_temps'.
        filename_suffix (str): The suffix/extension of the data files. Defaults to '.csv'.

    Returns:
        pd.DataFrame: DataFrame containing the previous day's data, with
                      'LocalTime' column parsed as datetime and set as index.
        None: If the file for the previous day is not found or an error occurs
              during reading or processing.
    """
    try:
        # 1. Determine yesterday's date string (UTC based)
        # Use UTC because the Lambda likely uses UTC for the 'run_time'/'date_col'
        today_utc = pd.Timestamp.utcnow().normalize() # Get date part only
        yesterday_utc = today_utc - timedelta(days=1)
        date_str = yesterday_utc.strftime('%Y-%m-%d')
        logger.info(f"Attempting to retrieve ERCOT temperature data for date: {date_str}")

        # 2. Construct the S3 path
        # Ensure prefix ends with a slash
        if s3_folder_prefix and not s3_folder_prefix.endswith('/'):
             s3_folder_prefix += '/'
        s3_path = f"s3://{bucket_name}/{s3_folder_prefix}{filename_prefix}_{date_str}{filename_suffix}"
        logger.info(f"Constructed S3 path: {s3_path}")

        # 3. Read the CSV from S3 using pandas
        logger.info(f"Reading CSV from {s3_path}...")

        # Attempt to read, parse 'LocalTime', and set it as index
        # Lambda script reset the index before saving, so 'LocalTime' is now a column
        df = pd.read_csv(
            s3_path,
            index_col='LocalTime',  # Specify the column to become the index
            parse_dates=['LocalTime'] # Tell pandas to parse this column as dates
        )

        logger.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df

    # Catch specific errors if needed, e.g., from s3fs or pandas
    except FileNotFoundError:
        # This will be raised by s3fs if the object doesn't exist
        logger.warning(f"File not found for previous day at {s3_path}. No data retrieved.")
        return None
    except KeyError as e:
         # This might happen if the 'LocalTime' column wasn't found in the CSV
         logger.error(f"Error reading {s3_path}. Likely missing expected column 'LocalTime'. Error: {e}", exc_info=True)
         logger.warning("Attempting to read CSV without setting 'LocalTime' as index...")
         # Fallback: Read without index setting
         try:
             df = pd.read_csv(s3_path)
             logger.info(f"Successfully read {len(df)} rows from {s3_path} (without index). Check columns.")
             return df
         except Exception as read_err:
             logger.error(f"Failed second attempt to read {s3_path}. Error: {read_err}", exc_info=True)
             return None
    except Exception as e:
        # Catch any other unexpected errors during S3 access or CSV parsing
        logger.error(f"An unexpected error occurred retrieving data from {s3_path}: {e}", exc_info=True)
        return None

