#!/usr/bin/env python
#-*- coding: utf-8 -*-

import argparse
import logging
import os
import boto3
import pandas as pd
import numpy as np
import datetime
import json

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def upload_dataframe_to_s3(
    df: pd.DataFrame,
    bucket_name: str,
    s3_folder_prefix: str,
    date_col: str = 'run_time',
    filename_prefix: str = 'ercot_temps',
    filename_suffix: str = '.csv'):
    """
    Uploads a Pandas DataFrame to a specific S3 folder, naming the file
    based on a date derived from a specified column. Creates a flat file
    structure within the target folder.

    Args:
        df (pd.DataFrame): The Pandas DataFrame to upload.
        bucket_name (str): The name of the S3 bucket.
        s3_folder_prefix (str): The full "folder" path within the bucket where
                                files will be stored (e.g., 'weather_forecasts/temperatures/ercot/').
                                Should end with a '/' if it's not empty.
        date_col (str): The name of the column in the DataFrame containing
                        the date/timestamp to use for filename generation
                        (defaults to 'run_time').
        filename_prefix (str): A prefix for the generated filename
                               (e.g., 'forecast_data').
        filename_suffix (str): The file extension or suffix (e.g., '.csv').

    Raises:
        ValueError: If the DataFrame is empty, the date column doesn't exist,
                    or the date column contains invalid date/timestamp data.
        Exception: Catches and reports errors during the S3 upload process.

    Example S3 structure:
        s3://your-bucket-name/weather_forecasts/temperatures/ercot/temperature_data_2025-04-19.csv
        s3://your-bucket-name/weather_forecasts/temperatures/ercot/temperature_data_2025-04-20.csv
        ...
    """
    logger.info("Starting S3 upload process (flat structure)...")

    # Input Validation
    if df.empty:
        logger.error("Input DataFrame is empty. Cannot upload.")
        raise ValueError("Input DataFrame is empty. Cannot upload.")
    if date_col not in df.columns:
        logger.error(f"Date column '{date_col}' not found in DataFrame.")
        raise ValueError(f"Date column '{date_col}' not found in DataFrame.")
    if not bucket_name:
         logger.error("Bucket name is not provided.")
         raise ValueError("Bucket name must be provided.")
    if not s3_folder_prefix:
         logger.error("S3 folder prefix is not provided.")
         raise ValueError("S3 folder prefix must be provided.")


    # Ensure the folder prefix ends with a slash
    if not s3_folder_prefix.endswith('/'):
        s3_folder_prefix += '/'

    # Determine Date for Filename
    try:
        file_date = pd.to_datetime(df[date_col].iloc[0])
        date_str = file_date.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error processing date column '{date_col}': {e}", exc_info=True)
        raise ValueError(
            f"Error processing partition column '{date_col}'. "
            f"Ensure it contains valid date/timestamp values. Original error: {e}"
        )

    # Construct Filename and S3 Path
    file_name = f"{filename_prefix}_{date_str}{filename_suffix}"
    s3_full_path = f"s3://{bucket_name}/{s3_folder_prefix}{file_name}"
    logger.info(f"Target S3 Path: {s3_full_path}")

    # Upload the DataFrame
    try:
        logger.info(f"Attempting to upload DataFrame ({len(df)} rows) to: {s3_full_path}")
        df_to_upload = df.copy()
        if isinstance(df_to_upload.index, pd.DatetimeIndex) or df_to_upload.index.name == 'LocalTime':
             logger.info("Resetting index 'LocalTime' to be included as a column in the CSV.")
             df_to_upload = df_to_upload.reset_index()

        df_to_upload.to_csv(s3_full_path, index=False)
        logger.info(f"Successfully uploaded DataFrame to {s3_full_path}")

    except Exception as e:
        logger.error(f"Error uploading to S3: {e}", exc_info=True)
        # Re-raise the exception so Lambda knows the execution failed
        raise e




def WSI_hourly_temp_scrape_and_store(wsi_user: str,
                                     wsi_pw: str,
                                     wsi_profile: str,
                                     cities: list,
                                     bucket_name: str,
                                     s3_folder_prefix: str,
                                     date_col: str,
                                     filename_prefix: str):

    """
    Scrapes WSI hourly forecast data and stores it to S3.
    Accepts configuration parameters directly.

    Parameters
    ----------
    cities : 'list'
        A list of the cities

    Returns
    -------
    None, stores data
    """
    logger.info(f"Starting WSI scrape for cities: {', '.join(cities)}")

    # --- Input Validation ---
    if not all([wsi_user, wsi_pw, wsi_profile]):
        logger.error("Missing WSI credentials.")
        raise ValueError("WSI credentials (user, password, profile) must be provided.")
    if not cities:
        logger.error("No cities provided for scraping.")
        raise ValueError("City list cannot be empty.")

    # WSI Data Portion
    city_fcst_data = pd.DataFrame()
    wsi_date_index = {}
    run_time = pd.Timestamp.utcnow().floor('D').tz_localize(None) # Calculate run_time once

    for station in cities:
        logger.info(f"Scraping data for station: {station}")
        # Use f-string formatting correctly
        wsi_url = (f'https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHourlyForecast?'
                   f'Account={wsi_user}&Profile={wsi_profile}&Password={wsi_pw}&'
                   f'TempUnits=F&SiteIds[]={station}&Region=NA')

        try:
            # Using pandas to read CSV directly from URL (needs requests library installed)
            wsi = pd.read_csv(wsi_url, header=[1], index_col=0)

            # Clean column names and set index type
            wsi.columns = wsi.columns.str.strip()
            wsi.index = pd.to_datetime(wsi.index)

            city_fcst_data[f'{station}_temp'] = wsi['Temp']
            city_fcst_data[f'{station}_dewpoint'] = wsi['DewPoint']
            wsi_date_index[f'{station}_time'] = wsi.index
            logger.info(f"Successfully processed data for {station}")

        except Exception as e:
            logger.error(f"Failed to scrape or process data for station {station} from {wsi_url}. Error: {e}", exc_info=True)
            # raise e # Option: Uncomment to stop processing on first failure
            continue # Option: Skip this station and continue with others

    # Check if any data was successfully scraped
    if city_fcst_data.empty:
         logger.error("No data was successfully scraped for any city. Aborting upload.")
         raise RuntimeError("Failed to scrape data for all specified cities.")

    # Test if the time spans are identical (optional but good check)
    try:
        date_df = pd.DataFrame.from_dict(wsi_date_index)
        # Simpler check: are all columns equal to the first column?
        is_consistent = date_df.apply(lambda x: x.equals(date_df.iloc[:, 0])).all()
        if not is_consistent:
            logger.warning('The city datetime indices are not identical. Proceeding anyway.')
    except Exception as e:
         logger.warning(f"Could not perform date consistency check: {e}")


    # Add run_time column before uploading
    logger.info(f"Assigning run_time: {run_time}")
    df_with_runtime = city_fcst_data.assign(**{date_col: run_time}) # Use date_col variable

    # Upload data to S3 - Pass arguments explicitly
    upload_dataframe_to_s3(
        df=df_with_runtime,
        bucket_name=bucket_name,
        s3_folder_prefix=s3_folder_prefix,
        date_col=date_col,
        filename_prefix=filename_prefix
    )
    logger.info("WSI scrape and store process completed.")


# --- AWS Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda entry point. Reads configuration from environment
    variables and triggers the scraping and storing process.
    """
    logger.info("Lambda function initiated.")
    logger.info(f"Received event: {json.dumps(event)}") # Log the trigger event if needed

    try:
        # --- Read Configuration from Environment Variables ---
        # Required S3 config
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        s3_folder_prefix = os.environ.get('S3_FOLDER_PREFIX')
        filename_prefix = os.environ.get('FILENAME_PREFIX', 'ercot_temps')
        date_col = os.environ.get('DATE_COLUMN_NAME', 'run_time')

        # Required WSI Credentials (from environment variables for now)
        # !! TODO: Migrate WSI_PW to AWS Secrets Manager !!
        wsi_user = os.environ.get('WSI_USER')
        wsi_pw = os.environ.get('WSI_PW') # Read password from environment
        wsi_profile = os.environ.get('WSI_PROFILE')

        # Required City List (expecting comma-separated string)
        cities_str = os.environ.get('CITIES_LIST', 'KMAF,KDFW,KAUS,KSAT,KIAH,KCRP') # Default list
        cities = [city.strip() for city in cities_str.split(',') if city.strip()]

        # --- Basic Validation of Environment Variables ---
        required_vars = {
            'S3_BUCKET_NAME': bucket_name,
            'S3_FOLDER_PREFIX': s3_folder_prefix,
            'WSI_USER': wsi_user,
            'WSI_PW': wsi_pw,
            'WSI_PROFILE': wsi_profile
        }
        missing_vars = [k for k, v in required_vars.items() if not v]
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not cities:
             error_msg = "CITIES_LIST environment variable is empty or invalid."
             logger.error(error_msg)
             raise ValueError(error_msg)

        logger.info(f"Configuration loaded: Bucket={bucket_name}, Prefix={s3_folder_prefix}, Cities={cities}")

        # --- Execute the Core Logic ---
        WSI_hourly_temp_scrape_and_store(
            cities=cities,
            wsi_user=wsi_user,
            wsi_pw=wsi_pw,
            wsi_profile=wsi_profile,
            bucket_name=bucket_name,
            s3_folder_prefix=s3_folder_prefix,
            date_col=date_col,
            filename_prefix=filename_prefix
        )

        logger.info("Lambda function execution successful.")
        return {
            'statusCode': 200,
            'body': json.dumps('Scraping and S3 upload completed successfully!')
        }

    except Exception as e:
        logger.error(f"Lambda function execution failed: {e}", exc_info=True)
        # Return error status for monitoring
        return {
            'statusCode': 500,
            # Provide error details in the body if desired, but be careful with sensitive info
            'body': json.dumps(f'Error during execution: {str(e)}')
        }



