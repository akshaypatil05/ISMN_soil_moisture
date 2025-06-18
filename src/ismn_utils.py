import pandas as pd
import numpy as np

def get_static(sm):

    static_data = []

    for sensor_idx in range(sm.dims['sensor']):
        sensor_data = {
            'sensor_id': sensor_idx,
            'network': sm.network.values[sensor_idx] if 'network' in sm.variables else 'AMMA-CATCH',
            'station': sm.station.values[sensor_idx] if 'station' in sm.variables else f'Station_{sensor_idx}',
            'sensor_name': str(sm.instrument.values[sensor_idx]) if 'instrument' in sm.variables else f'Sensor_{sensor_idx}',
            'latitude': float(sm.latitude.values[sensor_idx]),
            'longitude': float(sm.longitude.values[sensor_idx]),
            'elevation': float(sm.elevation.values[sensor_idx]) if 'elevation' in sm.variables else np.nan,
            'depth_from': float(sm.depth_from.values[sensor_idx]) if 'depth_from' in sm.variables else np.nan,
            'depth_to': float(sm.depth_to.values[sensor_idx]) if 'depth_to' in sm.variables else np.nan,
            'clay_fraction': float(sm.clay_fraction.values[sensor_idx]) if 'clay_fraction' in sm.variables else np.nan,
            'sand_fraction': float(sm.sand_fraction.values[sensor_idx]) if 'sand_fraction' in sm.variables else np.nan,
            'silt_fraction': float(sm.silt_fraction.values[sensor_idx]) if 'silt_fraction' in sm.variables else np.nan,
            'organic_carbon': float(sm.organic_carbon.values[sensor_idx]) if 'organic_carbon' in sm.variables else np.nan,
        }
        static_data.append(sensor_data)

    static_df = pd.DataFrame(static_data)

    return static_df


def get_sm_time_series(sm, statistic='mean'):
    """
    Extracts time series of soil moisture data with a specified statistical operation.
    
    Parameters:
    - sm: xarray dataset of soil moisture
    - statistic: str, one of ['mean', 'median', 'min', 'max', 'sum', 'std']
    
    Returns:
    - ts_df: pandas DataFrame with time series data
    """
    # Convert time to datetime
    sm_with_time = sm.assign_coords(date_time=pd.to_datetime(sm.date_time.values))
    
    # Select the aggregation method
    if statistic == 'mean':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').mean()
    elif statistic == 'median':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').median()
    elif statistic == 'min':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').min()
    elif statistic == 'max':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').max()
    elif statistic == 'sum':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').sum()
    elif statistic == 'std':
        daily_sm = sm_with_time.soil_moisture.resample(date_time='D').std()
    else:
        raise ValueError(f"Statistic '{statistic}' is not supported. Use 'mean', 'median', 'min', 'max', 'sum', or 'std'.")
    
    # Prepare time series
    dates = pd.to_datetime(daily_sm.date_time.values)
    date_strings = [d.strftime('%Y-%m-%d') for d in dates]
    sm_values = daily_sm.values  
    
    # Cleaning
    valid_mask = (
        (~np.isnan(sm_values)) & 
        (sm_values >= 0) & 
        (sm_values <= 1) & 
        (sm_values != -9999) & 
        (sm_values != -999)
    )
    sm_values_clean = np.where(valid_mask, sm_values, np.nan)

    # Build DataFrame
    ts_df = pd.DataFrame(sm_values_clean, columns=date_strings)
    
    return ts_df



def export_gdf(gdf, output_path, file_format='geojson'):
    """
    Export a GeoDataFrame to the specified format.

    Parameters:
    - gdf: GeoDataFrame to export
    - output_path: Path without file extension
    - file_format: 'geojson', 'shp', 'parquet', 'gpkg'
    """
    file_format = file_format.lower()

    supported_formats = ['geojson', 'shp', 'parquet', 'gpkg','csv']

    if file_format not in supported_formats:
        raise ValueError(f"Unsupported format: {file_format}. Supported formats are: {supported_formats}")

    # Set file extension based on format
    if file_format == 'geojson':
        full_path = f"{output_path}.geojson"
        driver = 'GeoJSON'
        gdf.to_file(full_path, driver=driver)

    elif file_format == 'shp':
        full_path = f"{output_path}.shp"
        driver = 'ESRI Shapefile'
        gdf.to_file(full_path, driver=driver)

    elif file_format == 'gpkg':
        full_path = f"{output_path}.gpkg"
        driver = 'GPKG'
        gdf.to_file(full_path, driver=driver)

    elif file_format == 'parquet':
        full_path = f"{output_path}.parquet"
        gdf.to_parquet(full_path)
    
    elif file_format=='csv':
        full_path=f'{output_path}.csv'
        gdf.to_csv(full_path)

    print(f"File successfully written to {full_path}\n{'-' * 50}")

