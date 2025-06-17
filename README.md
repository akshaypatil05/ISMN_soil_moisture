
# ISMN Soil Moisture Time Series Extraction and Export

A Python tool to extract, process, and export soil moisture time series from ISMN (International Soil Moisture Network) datasets for multiple networks and stations.

## Sign up in ISMN portal

* Create an account on ISMN portal to access the data (https://ismn.earth/en/)
* After login click on DATA ACCESS (https://ismn.earth/en/dataviewer/)
 * Select Network
 * Date range
 * Sensor (All Variables, Depth)
    * All Variables (soil moisture, temperature, precipitation, surface temperature, soil suction)
    * Depth (select depth range)
 * Select All climates and All landcovers
 * Hit the Downlaod button
    * Review the request
    * Choose Format: Header+values
    * Tick Gap Filling
    * Tick Quality flags
    * Proceed for download
---


## Project Structure

```
ISMN_STATIONS/
├── data/                      # Input ISMN data and output files
├── src/                       # Python source code
│   ├── ismn_utils.py # Core extraction and export functions
├── notebooks/ 
|___test_codes/                # Jupyter notebooks for experiments
├── README.md                  # Project documentation
└── requirements.txt           # Python dependencies
```

---

## Features

* Extracts static parameters at station level.
* Computes daily soil moisture time series using multiple statistics
  * Mean
  * Median
  * Minimum
  * Maximum
  * Standard Deviation

* Supports multiple export formats:
  * Shapefile (`.shp`)
  * GeoJSON (`.geojson`)
  * Parquet (`.parquet`)
  * GeoPackage (`.gpkg`)
  * CSV (`.csv`)
* Automatically creates directories for outputs.

---

## Installation

1. Clone the repository:

```bash
git clone https://akshay_encode@bitbucket.org/encodenature/ismn_soil_moisture.git
cd ismn_soil_moisture
```

2. Set up a Python environment:

```bash
conda create -n ismn_env python=3.9
conda activate ismn_env
```

3. Install the required packages:

```bash
pip install -r requirements.txt 
```
OR

```bash
pip install ismn 
```
---

## Usage

### Running the Extraction

```python
for network in ismn_data.networks:
    print(f'Processing network: {network}')
    sm = ismn_data[network].to_xarray(variable='soil_moisture')

    static_df = get_static(sm)
    ts_df = get_sm_time_series(sm, statistic='mean')  # Supported: mean, median, min, max, sum, std

    merged_df = pd.concat([static_df, ts_df], axis=1)

    geometry = [Point(xy) for xy in zip(merged_df['longitude'], merged_df['latitude'])]
    gdf = gpd.GeoDataFrame(merged_df, geometry=geometry, crs='EPSG:4326')

    output_path = os.path.join(os.path.split(path)[0], 'extracted_data', f'{network}')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    export_gdf(gdf, output_path, file_format='geojson')  # Supported formats: geojson, shp, parquet, gpkg, csv
```

### Example: Computing Standard Deviation

```python
ts_df = get_sm_time_series(sm, statistic='std')
```

---

## Export Function

```python
export_gdf(gdf, output_path, file_format='geojson')
```

**Arguments:**

* `gdf`: GeoDataFrame to export.
* `output_path`: Path without extension.
* `file_format`: File type: 'shp', 'geojson', 'parquet', 'gpkg', 'csv'.
---

## Future Improvements

* Weekly and monthly aggregations.
* Custom user-defined statistics (e.g. percentiles).
* Multi-parameter extraction (e.g. temperature, precipitation).

---

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

---

## Contact

**Dr. Akshay Patil**
\[akshapatil1989@gmail.com] | 

---

## License

\[Open License]
