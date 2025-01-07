import pandas as pd
import logging

logger = logging.getLogger(__name__)

def _get_vertical_coordinate_name(ds, vertical_level_id, base_coords_names):
    vertical_coordinate = list(set(list(ds[vertical_level_id].coords)) - set(base_coords_names))[0]
    return vertical_coordinate

import xarray as xr

def select_region(ds, lat_min, lat_max, lon_min, lon_max):
    """
    Selects a geographic region from the dataset, handling longitude wrapping if necessary.

    Args:
        ds (xarray.Dataset or xarray.DataArray): The dataset or data array to select from.
        lat_min (float): Minimum latitude (southern boundary).
        lat_max (float): Maximum latitude (northern boundary).
        lon_min (float): Minimum longitude (western boundary, in 0-360 format).
        lon_max (float): Maximum longitude (eastern boundary, in 0-360 format).

    Returns:
        xarray.Dataset or xarray.DataArray: The selected region.
    """
    if lon_min <= lon_max:
        # Standard case: no wrapping needed
        return ds.sel(
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max)
        )
    else:
        # Wrapping case: split into two ranges and concatenate
        ds_part_1 = ds.sel(
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, 360)  # From lon_min to 360
        )
        ds_part_2 = ds.sel(
            latitude=slice(lat_max, lat_min),
            longitude=slice(0, lon_max)  # From 0 to lon_max
        )
        # Combine the two parts along the longitude axis
        return xr.concat([ds_part_1, ds_part_2], dim="longitude")

def reshape_level_variables(df, id_vars, value_vars, level_var):
    """
    Transforms a DataFrame by appending level to variable names.

    Args:
        df (pd.DataFrame): Input DataFrame.
        id_vars (list): List of columns to keep as identifiers (e.g., coordinates, time).
        value_vars (list): List of variables to combine with the pressure level.
        level_var (str): Column name representing the levels.

    Returns:
        pd.DataFrame: Reshaped DataFrame with levels appended to variable names.
    """
    # Melt the DataFrame to make variables and pressure levels into rows
    df_melted = df.melt(
        id_vars=id_vars + [level_var],
        value_vars=value_vars,
        var_name='variable',
        value_name='value'
    )

    # Combine variable names with pressure levels
    df_melted['variable'] = df_melted['variable'] + '_' + df_melted[level_var].astype(int).astype(str)

    # Drop the pressure level column after merging it with variable names
    df_melted = df_melted.drop(columns=[level_var])

    # Pivot the DataFrame back to a wide format
    df_final = df_melted.pivot_table(
        index=id_vars,
        columns='variable',
        values='value'
    ).reset_index()

    # Remove multi-index from columns
    df_final.columns.name = None

    return df_final

def process(ds, lat_min, lat_max, lon_min, lon_max, pass_list, base_coords_names):
    """
    Processes the dataset by reshaping and merging variables across vertical levels.

    Args:
        ds (xarray.Dataset): Input dataset with multiple vertical levels.
        lat_min (float): Minimum latitude for filtering.
        lat_max (float): Maximum latitude for filtering.
        lon_min (float): Minimum longitude for filtering.
        lon_max (float): Maximum longitude for filtering.
        pass_list (list): List of variables to ignore during processing.
        base_coords_names (list): List of base coordinate names.

    Returns:
        pd.DataFrame: A single DataFrame containing all processed variables.
    """

    result_frames = []  # List to store intermediate DataFrames

    for vertical_level_id in range(len(ds)):
        variables = [name for name in ds[vertical_level_id].data_vars]
        vertical_coordinate = _get_vertical_coordinate_name(ds, vertical_level_id, base_coords_names)

        # Skip unwanted variables or nominal top levels
        if set(variables).issubset(set(pass_list)) or vertical_coordinate == 'nominalTop':
            continue

        # Select the relevant geographic region
        ds_selected = select_region(ds[vertical_level_id], lat_min, lat_max, lon_min, lon_max)

        if vertical_coordinate == 'isobaricInhPa':
            # Filter specific pressure levels
            ds_selected = ds_selected.sel(isobaricInhPa=[1000, 500])
            df = ds_selected.to_dataframe().reset_index()

            # Reshape variables for isobaric levels
            id_vars = ['latitude', 'longitude', 'time', 'step', 'valid_time']
            value_vars = variables
            df = reshape_level_variables(df, id_vars=id_vars, value_vars=value_vars, level_var='isobaricInhPa')
        else:
            # Convert other levels to DataFrame and drop vertical coordinate
            df = ds_selected.to_dataframe().reset_index()
            df.drop(vertical_coordinate, axis=1, inplace=True)

        # Append the resulting DataFrame to the list
        result_frames.append(df)

        # Merge all intermediate DataFrames into a single DataFrame
        if len(result_frames) > 1:
            final_result = result_frames[0]
            for df in result_frames[1:]:
                final_result = pd.merge(
                    final_result,
                    df,
                    on=['latitude', 'longitude', 'time', 'step', 'valid_time'],
                    how='outer'
                )
        elif len(result_frames) == 1:
            final_result = result_frames[0]
        else:
            logger.warning("No data frames to merge. Returning an empty DataFrame.")
            final_result = pd.DataFrame()

    return final_result
    
