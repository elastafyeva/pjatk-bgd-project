# pjatk-bgd-project

## Project Description

This project is designed for downloading, processing, and analyzing Numerical Weather Forecast data using Python. For more details about the `herbie` library, refer to the [Herbie documentation](https://herbie.readthedocs.io/en/stable/index.html).

## Usage

1. Download the repository and navigate to its directory.
2. Ensure the dependencies are installed.
3. Run `main.py` with one of the modes:
   - To download archived data:
     ```bash
     python main.py archive
     ```
   - To download the latest data:
     ```bash
     python main.py latest
     ```
   - For test mode:
     ```bash
     python main.py test
     ```
4. Processed results will be saved in the `data/` directory in CSV format.


## File Structure

There are three main scripts:

- **`main.py`**: The primary script for managing data download and processing workflows.
- **`downloader.py`**: Contains functions for downloading data.
- **`processing.py`**: Contains functions for processing the downloaded data.

### `main.py`

- Handles the downloading and processing workflows.
- Supports three modes:
  - `archive`: Downloads archived data for a specified time range.
  - `latest`: Downloads the latest available data.
  - `test`: A test mode with a limited dataset.
- Configures logging and manages temporary directories.
- Uses multithreading for parallel file processing.

### `downloader.py`

- Contains functions:
  - `download_archive`: Downloads data for a specified date range.
  - `download_latest`: Downloads the latest available data.
- Integrates with the `herbie` library for working with meteorological data.
- Prepares task lists for processing the downloaded files.

### `processing.py`

- Contains functions:
  - `process`: Processes the downloaded data, including filtering by geographic parameters and converting data into a user-friendly format.
  - `_get_vertical_coordinate_name`: Identifies the vertical coordinate in the dataset.
  - `reshape_level_variables`: Ensures all variables have consistent shapes by splitting variables across multiple levels into separate variables named accordingly. For example, temperature t becomes t_1000, t_500, etc., based on the levels.
- Supports working with multi-layer datasets.

## Dependencies

The project requires the following libraries:

- `os`
- `sys`
- `shutil`
- `logging`
- `datetime`
- `multiprocessing`
- `concurrent.futures`
- `cfgrib`
- `pandas`
- `herbie`

Install the dependencies using:

```bash
pip install -r requirements.txt
```

## Configuration Example

Parameters that can be configured in `main.py`:

- **Model:**
  ```python
  model = "gefs"
  ```
- **Product:**
  ```python
  product = "atmos.5"
  ```
- **Ensemble Member:**
  ```python
  member = "mean"
  ```
- **Forecasts:**
  Forecast horizons: start, end, and step in hours.
  ```python
  fxx = range(0, 24, 6)
  ```
- **Geographic Parameters:**
  ```python
  lat_min, lat_max = 20, 70
  lon_min, lon_max = 180, 210
  ```
- **Filtered Variables:**
  Shorthand names of variables that are not of interest and can be skipped.
  ```python
  pass_list = ['tcc', 'st', 'soilw', 'w']
  ```

## Logging

Scripts use the `logging` module to output progress information. Logs are displayed in the console in the format:

```
YYYY-MM-DD HH:MM:SS - LEVEL - Message
```

## Cleanup

Temporary files are automatically removed from the `temp_weather_data` directory after execution.

## Support

For questions and suggestions, contact at [eastafeva12@gmail.com](mailto:eastafeva12@gmail.com) or [@elastafyeva](https://t.me/elastafyeva) on Telegram.


