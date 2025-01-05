from downloader import *
import os
import sys
import shutil
import logging
from datetime import datetime
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

# Logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    # Directory for downloaded files
    data_directory = 'temp_weather_data'
    os.makedirs(data_directory, exist_ok=True) # for raw files
    os.makedirs('data', exist_ok=True) # for processed files

    # Parameters for downloading and processing
    model = "gefs"
    product = "atmos.5"
    member = "mean"
    fxx = range(0, 24, 6)

    # Geographic and filtering parameters
    lat_min, lat_max = 20, 70
    lon_min, lon_max = 180, 210
    pass_list = ['tcc', 'st', 'soilw', 'w']
    base_coords_names = ['latitude', 'longitude', 'time', 'step', 'valid_time']

    if mode == "archive":
        start_date = datetime(2025, 1, 5, 0, 0)
        end_date = datetime(2025, 1, 5, 12, 0)
        tasks = download_archive(start_date, end_date, model, product, member, fxx, data_directory,
        (lat_min, lat_max, lon_min, lon_max, pass_list, base_coords_names))

    elif mode == "latest":
        tasks = download_latest(model, product, member, fxx, data_directory,
        (lat_min, lat_max, lon_min, lon_max, pass_list, base_coords_names))


    elif mode == "test":
        fxx = list(fxx)[:2]
        tasks = download_latest(model, product, member, fxx, data_directory,
        (lat_min, lat_max, lon_min, lon_max, pass_list, base_coords_names))

    # Parallel execution
    max_workers = min(5, multiprocessing.cpu_count()) # Use at most 5 threads or CPU count
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(preprocess, tasks))

    for result in results:
        logger.info(result)

    logger.info("All files downloaded and processed.")

    # Remove all files and directories in temp_weather_data
    try:
        shutil.rmtree(data_directory)
        logger.info(f"Removed all files and directories in {data_directory}.")
    except Exception as e:
        logger.error(f"Error while cleaning up {data_directory}: {e}")
