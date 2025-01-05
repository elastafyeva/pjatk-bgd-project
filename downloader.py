from processing import process
import os
import logging
import cfgrib
import pandas as pd
from herbie import FastHerbie, HerbieLatest
from datetime import datetime

logger = logging.getLogger(__name__)

def download_archive(start_date, end_date, model, product, member, fxx, data_directory, other_params):
    try:
        logger.info(f"Creating FastHerbie object for range {start_date} to {end_date}")
        FH = FastHerbie(
            pd.date_range(start=start_date, end=end_date, freq="6h"),
            model=model,
            product=product,
            member = member,
            fxx=fxx,
            save_dir=data_directory
        )
        file_paths = FH.download()

        tasks = [(os.path.join(root, file), *other_params) for root, _, files in os.walk(data_directory) for file in files]
        return tasks
    except Exception as e:
        logger.error(f"Error initializing FastHerbie: {e}")
        return []

def download_latest(model, product, member, fxx, data_directory, other_params):
    try:
        now = datetime.now()
        logger.info(f"Creating HerbieLatest object for {now}")
        HL = HerbieLatest(
            model=model,
            product=product,
            member=member
        )
        latest_date = HL.date
        FH = FastHerbie(
            pd.date_range(start=latest_date, end=latest_date, freq="6h"),
            model=model,
            product=product,
            member=member,
            fxx=fxx,
            save_dir=data_directory
        )

        file_paths = FH.download()

        tasks = [(os.path.join(root, file), *other_params) for root, _, files in os.walk(data_directory) for file in
                 files]
        return tasks
    except Exception as e:
        logger.error(f"Error initializing HerbieLatest: {e}")
        return []

def preprocess(task):
    try:
        file_path, lat_min, lat_max, lon_min, lon_max, pass_list, base_coords_names = task

        logger.info(f"Processing file: {file_path}")
        ds = cfgrib.open_datasets(file_path)

        # Use the process function
        final_df = process(
            ds=ds,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            pass_list=pass_list,
            base_coords_names=base_coords_names
        )

        # Save the processed DataFrame to a CSV file
        file_name = '_'.join(str(file_path).split('/')[-1].split('.'))
        csv_file_path = os.path.join(f"data/processed_{file_name}.csv")
        final_df.to_csv(csv_file_path, index=False)
        logger.info(f"Processed data saved to: {csv_file_path}")

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return f"Error: {file_path}"

