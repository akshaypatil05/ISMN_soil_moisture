#!/usr/bin/env python3
"""
ISMN Simple Direct Processor - Windows Path Length Fix
Processes ISMN data directly from zip file without extraction
"""

import os
import zipfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point
import warnings
warnings.filterwarnings('ignore')

class ISMNSimpleProcessor:
    def __init__(self, zip_path, output_dir="output"):
        """Initialize ISMN processor"""
        self.zip_path = zip_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.sensors_data = []
        
    def process_zip_directly(self):
        """Process zip file directly without extraction"""
        print(f"Processing {self.zip_path} directly...")
        
        if not os.path.exists(self.zip_path):
            raise FileNotFoundError(f"Zip file not found: {self.zip_path}")
        
        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            sensor_files = [f for f in file_list if not f.endswith('/') and 
                           (f.endswith('.stm') or f.endswith('.csv') or 'sm_' in f.lower())]
            
            print(f"Found {len(sensor_files)} sensor files in zip")
            
            for i, file_path in enumerate(sensor_files):
                if i % 100 == 0:  # Progress indicator
                    print(f"Processing file {i+1}/{len(sensor_files)}")
                
                try:
                    self.process_single_file(zip_ref, file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                    continue
        
        print(f"Successfully processed {len(self.sensors_data)} sensors")
        
    def process_single_file(self, zip_ref, file_path):
        """Process a single sensor file from the zip"""
        # Parse path structure
        path_parts = file_path.replace('\\', '/').split('/')
        path_parts = [p for p in path_parts if p]  # Remove empty parts
        
        if len(path_parts) < 2:
            return
            
        # Extract identifiers from path
        network_name = path_parts[0] if len(path_parts) > 0 else "Unknown"
        station_name = path_parts[1] if len(path_parts) > 1 else "Unknown"
        filename = path_parts[-1]
        
        # Clean up names (remove common prefixes/suffixes)
        network_clean = self.clean_name(network_name)
        station_clean = self.clean_name(station_name)
        sensor_id = os.path.splitext(filename)[0]
        
        # Read file content
        try:
            with zip_ref.open(file_path) as file_content:
                content = file_content.read().decode('utf-8', errors='ignore')
                lines = content.split('\n')[:100]  # First 100 lines for metadata
        except Exception as e:
            print(f"Could not read {file_path}: {str(e)}")
            return
            
        # Extract coordinates
        coordinates = self.extract_coordinates(lines)
        
        if coordinates:
            sensor_info = {
                'Sensor_ID': sensor_id,
                'Station_ID': station_clean,
                'Network_ID': network_clean,
                'Network_Name': network_clean,
                'Latitude': coordinates['lat'],
                'Longitude': coordinates['lon'],
                'File_Path': file_path
            }
            self.sensors_data.append(sensor_info)
            
    def clean_name(self, name):
        """Clean up network/station names"""
        # Remove common suffixes and prefixes
        cleaned = name.replace('_', ' ').strip()
        # Take first meaningful part if too long
        if len(cleaned) > 50:
            cleaned = cleaned.split()[0]
        return cleaned
        
    def extract_coordinates(self, lines):
        """Extract latitude and longitude from file lines"""
        lat, lon = None, None
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                # Check header lines for coordinates
                if '#' in line:
                    self.parse_header_coordinates(line, locals())
                continue
                
            # Look for coordinate patterns
            if any(keyword in line.lower() for keyword in ['latitude', 'lat', 'longitude', 'lon']):
                coords = re.findall(r'[-+]?\d*\.?\d+', line)
                if coords:
                    for coord in coords:
                        val = float(coord)
                        if -90 <= val <= 90 and lat is None:
                            lat = val
                        elif -180 <= val <= 180 and lon is None:
                            lon = val
                            
            # Try to find two numbers that could be coordinates
            if lat is None or lon is None:
                numbers = re.findall(r'[-+]?\d+\.?\d*', line)
                if len(numbers) >= 2:
                    for i in range(len(numbers)-1):
                        try:
                            val1, val2 = float(numbers[i]), float(numbers[i+1])
                            if (-90 <= val1 <= 90 and -180 <= val2 <= 180 and 
                                lat is None and lon is None):
                                lat, lon = val1, val2
                                break
                        except:
                            continue
                            
            if lat is not None and lon is not None:
                break
                
        # Additional parsing for ISMN specific formats
        if lat is None or lon is None:
            lat, lon = self.parse_ismn_specific_format(lines)
            
        if lat is not None and lon is not None:
            return {'lat': lat, 'lon': lon}
        return None
        
    def parse_header_coordinates(self, line, local_vars):
        """Parse coordinates from header lines"""
        # Common ISMN header patterns
        if 'station' in line.lower():
            coords = re.findall(r'[-+]?\d+\.?\d+', line)
            if len(coords) >= 2:
                try:
                    lat_val, lon_val = float(coords[0]), float(coords[1])
                    if -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
                        local_vars['lat'], local_vars['lon'] = lat_val, lon_val
                except:
                    pass
                    
    def parse_ismn_specific_format(self, lines):
        """Parse ISMN specific coordinate formats"""
        lat, lon = None, None
        
        for line in lines:
            # Look for specific ISMN patterns
            if line.startswith('#') and ('station' in line.lower() or 'location' in line.lower()):
                # Extract all numbers from the line
                numbers = re.findall(r'[-+]?\d+\.?\d+', line)
                if len(numbers) >= 2:
                    for i in range(len(numbers)-1):
                        try:
                            val1, val2 = float(numbers[i]), float(numbers[i+1])
                            if -90 <= val1 <= 90 and -180 <= val2 <= 180:
                                lat, lon = val1, val2
                                break
                        except:
                            continue
                            
        return lat, lon
        
    def create_shapefile(self, output_filename="ismn_sensors.shp"):
        """Create shapefile from processed data"""
        if not self.sensors_data:
            raise ValueError("No sensor data found. Run process_zip_directly() first.")
            
        # Create DataFrame
        df = pd.DataFrame(self.sensors_data)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['Sensor_ID', 'Station_ID', 'Network_ID', 'Latitude', 'Longitude'])
        
        # Create geometry
        geometry = [Point(xy) for xy in zip(df['Longitude'], df['Latitude'])]
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
        
        # Save shapefile
        output_path = self.output_dir / output_filename
        gdf.to_file(output_path)
        
        print(f"\nShapefile created: {output_path}")
        print(f"Total sensors: {len(gdf)}")
        print(f"Networks: {gdf['Network_Name'].nunique()}")
        print(f"Stations: {gdf['Station_ID'].nunique()}")
        
        # Save CSV as well
        csv_path = self.output_dir / f"{output_filename.replace('.shp', '.csv')}"
        df.to_csv(csv_path, index=False)
        print(f"CSV saved: {csv_path}")
        
        return gdf
        
    def print_summary(self):
        """Print processing summary"""
        if not self.sensors_data:
            print("No data to summarize")
            return
            
        df = pd.DataFrame(self.sensors_data)
        
        print("\n" + "="*60)
        print("ISMN DATA PROCESSING SUMMARY")
        print("="*60)
        print(f"Total Sensors: {len(df)}")
        print(f"Total Stations: {df['Station_ID'].nunique()}")
        print(f"Total Networks: {df['Network_ID'].nunique()}")
        
        print(f"\nCoordinate Range:")
        print(f"Latitude: {df['Latitude'].min():.4f} to {df['Latitude'].max():.4f}")
        print(f"Longitude: {df['Longitude'].min():.4f} to {df['Longitude'].max():.4f}")
        
        print(f"\nTop Networks by Sensor Count:")
        network_counts = df['Network_Name'].value_counts().head()
        for network, count in network_counts.items():
            print(f"  {network}: {count} sensors")

def process_ismn_simple(zip_path, output_dir="output", shapefile_name="ismn_sensors.shp"):
    """
    Simple function to process ISMN data
    
    Parameters:
    zip_path (str): Path to ISMN zip file
    output_dir (str): Output directory
    shapefile_name (str): Name of output shapefile
    
    Returns:
    GeoDataFrame: Processed sensor data
    """
    processor = ISMNSimpleProcessor(zip_path, output_dir)
    processor.process_zip_directly()
    gdf = processor.create_shapefile(shapefile_name)
    processor.print_summary()
    
    return gdf

# Example usage:
"""
# Use this function directly
zip_file = "your_ismn_data.zip"
gdf = process_ismn_simple(zip_file, "output", "sensors.shp")
print(gdf.head())
"""