#!/usr/bin/env python3
"""
ISMN Improved Coordinate Parser
Fixes coordinate extraction issues that cause vertical line artifacts
"""

import os
import zipfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
import re
from shapely.geometry import Point
import warnings
import numpy as np
warnings.filterwarnings('ignore')

class ISMNImprovedProcessor:
    def __init__(self, zip_path, output_dir="output"):
        """Initialize ISMN processor"""
        self.zip_path = zip_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.sensors_data = []
        self.debug_info = []
        
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
                if i % 100 == 0:
                    print(f"Processing file {i+1}/{len(sensor_files)}")
                
                try:
                    self.process_single_file(zip_ref, file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                    continue
        
        print(f"Successfully processed {len(self.sensors_data)} sensors")
        self.analyze_coordinate_patterns()
        
    def process_single_file(self, zip_ref, file_path):
        """Process a single sensor file from the zip"""
        # Parse path structure
        path_parts = file_path.replace('\\', '/').split('/')
        path_parts = [p for p in path_parts if p]
        
        if len(path_parts) < 2:
            return
            
        # Extract identifiers
        network_name = path_parts[0]
        station_name = path_parts[1] if len(path_parts) > 1 else path_parts[0]
        filename = path_parts[-1]
        
        # Clean names
        network_clean = self.clean_name(network_name)
        station_clean = self.clean_name(station_name)
        sensor_id = os.path.splitext(filename)[0]
        
        # Read file content
        try:
            with zip_ref.open(file_path) as file_content:
                content = file_content.read().decode('utf-8', errors='ignore')
                lines = content.split('\n')
        except Exception as e:
            return
            
        # Extract coordinates with improved method
        coordinates = self.extract_coordinates_improved(lines, file_path)
        
        if coordinates:
            sensor_info = {
                'Sensor_ID': sensor_id,
                'Station_ID': station_clean,
                'Network_ID': network_clean,
                'Network_Name': network_clean,
                'Latitude': coordinates['lat'],
                'Longitude': coordinates['lon'],
                'File_Path': file_path,
                'Coordinate_Source': coordinates.get('source', 'unknown')
            }
            self.sensors_data.append(sensor_info)
            
    def clean_name(self, name):
        """Clean up network/station names"""
        cleaned = name.replace('_', ' ').strip()
        if len(cleaned) > 50:
            cleaned = cleaned.split()[0]
        return cleaned
        
    def extract_coordinates_improved(self, lines, file_path):
        """Improved coordinate extraction with multiple strategies"""
        
        # Strategy 1: Look for explicit coordinate headers
        coords = self.find_header_coordinates(lines)
        if coords:
            coords['source'] = 'header'
            return coords
            
        # Strategy 2: Parse ISMN-specific metadata format
        coords = self.parse_ismn_metadata(lines)
        if coords:
            coords['source'] = 'ismn_metadata'
            return coords
            
        # Strategy 3: Look for coordinate patterns in data
        coords = self.find_coordinate_patterns(lines)
        if coords:
            coords['source'] = 'pattern_match'
            return coords
            
        # Strategy 4: Extract from filename if it contains coordinates
        coords = self.extract_from_filename(file_path)
        if coords:
            coords['source'] = 'filename'
            return coords
            
        # Debug: Store failed cases
        self.debug_info.append({
            'file': file_path,
            'first_10_lines': lines[:10],
            'issue': 'no_coordinates_found'
        })
        
        return None
        
    def find_header_coordinates(self, lines):
        """Find coordinates in header lines"""
        lat, lon = None, None
        
        for line in lines[:50]:  # Check first 50 lines
            line = line.strip()
            if not line:
                continue
                
            # Look for explicit latitude/longitude labels
            if re.search(r'latitude\s*[=:]\s*([-+]?\d+\.?\d*)', line, re.IGNORECASE):
                match = re.search(r'latitude\s*[=:]\s*([-+]?\d+\.?\d*)', line, re.IGNORECASE)
                lat_val = float(match.group(1))
                if -90 <= lat_val <= 90:
                    lat = lat_val
                    
            if re.search(r'longitude\s*[=:]\s*([-+]?\d+\.?\d*)', line, re.IGNORECASE):
                match = re.search(r'longitude\s*[=:]\s*([-+]?\d+\.?\d*)', line, re.IGNORECASE)
                lon_val = float(match.group(1))
                if -180 <= lon_val <= 180:
                    lon = lon_val
                    
        if lat is not None and lon is not None:
            return {'lat': lat, 'lon': lon}
        return None
        
    def parse_ismn_metadata(self, lines):
        """Parse ISMN specific metadata formats"""
        lat, lon = None, None
        
        for line in lines[:100]:
            line = line.strip()
            
            # Common ISMN patterns
            if line.startswith('#') and 'station' in line.lower():
                # Extract coordinates from station info lines
                coords = self.extract_coordinate_pair(line)
                if coords:
                    return coords
                    
            # Check for coordinate blocks
            if any(keyword in line.lower() for keyword in ['location', 'coordinates', 'position']):
                coords = self.extract_coordinate_pair(line)
                if coords:
                    return coords
                    
            # Look for decimal degree patterns
            if re.search(r'[-+]?\d+\.\d{4,}', line):  # Look for high precision decimals
                coords = self.extract_coordinate_pair(line)
                if coords:
                    return coords
                    
        return None
        
    def extract_coordinate_pair(self, line):
        """Extract a coordinate pair from a line"""
        # Find all decimal numbers in the line
        numbers = re.findall(r'[-+]?\d+\.?\d*', line)
        
        if len(numbers) < 2:
            return None
            
        # Try different combinations of numbers
        for i in range(len(numbers) - 1):
            try:
                val1, val2 = float(numbers[i]), float(numbers[i + 1])
                
                # Check if they could be lat/lon
                if self.is_valid_coordinate_pair(val1, val2):
                    return {'lat': val1, 'lon': val2}
                    
                # Try swapped
                if self.is_valid_coordinate_pair(val2, val1):
                    return {'lat': val2, 'lon': val1}
                    
            except (ValueError, IndexError):
                continue
                
        return None
        
    def is_valid_coordinate_pair(self, lat, lon):
        """Check if a pair of numbers could be valid coordinates"""
        # Basic range check
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return False
            
        # Additional validation: avoid obviously wrong values
        if lat == 0 and lon == 0:  # Null island - likely default/error
            return False
            
        if abs(lat) < 0.001 or abs(lon) < 0.001:  # Too close to zero
            return False
            
        # Check for common error patterns
        if lat == lon:  # Same value for both - likely error
            return False
            
        if abs(lat - int(lat)) < 0.0001 and abs(lon - int(lon)) < 0.0001:
            # Both are integers - might be wrong unless they're actually integer coordinates
            return True
            
        return True
        
    def find_coordinate_patterns(self, lines):
        """Find coordinate patterns in data lines"""
        # Look for lines with exactly two decimal numbers
        for line in lines[1:20]:  # Skip header, check first data lines
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            # Split by common delimiters
            parts = re.split(r'[,\s\t]+', line)
            
            if len(parts) >= 2:
                try:
                    # Try first two values
                    val1, val2 = float(parts[0]), float(parts[1])
                    if self.is_valid_coordinate_pair(val1, val2):
                        return {'lat': val1, 'lon': val2}
                except (ValueError, IndexError):
                    continue
                    
        return None
        
    def extract_from_filename(self, file_path):
        """Try to extract coordinates from filename if present"""
        filename = os.path.basename(file_path)
        
        # Look for coordinate patterns in filename
        coord_pattern = r'([-+]?\d+\.?\d*)_([-+]?\d+\.?\d*)'
        match = re.search(coord_pattern, filename)
        
        if match:
            try:
                val1, val2 = float(match.group(1)), float(match.group(2))
                if self.is_valid_coordinate_pair(val1, val2):
                    return {'lat': val1, 'lon': val2}
            except ValueError:
                pass
                
        return None
        
    def analyze_coordinate_patterns(self):
        """Analyze coordinate patterns to identify issues"""
        if not self.sensors_data:
            return
            
        df = pd.DataFrame(self.sensors_data)
        
        print("\n" + "="*60)
        print("COORDINATE ANALYSIS")
        print("="*60)
        
        # Check for suspicious patterns
        lat_rounded = df['Latitude'].round(3)
        lon_rounded = df['Longitude'].round(3)
        
        # Find repeated coordinates
        repeated_coords = df.groupby(['Latitude', 'Longitude']).size()
        suspicious_repeats = repeated_coords[repeated_coords > 10]
        
        if len(suspicious_repeats) > 0:
            print("⚠️  Suspicious coordinate repeats (>10 sensors at same location):")
            for (lat, lon), count in suspicious_repeats.head().items():
                print(f"   {lat:.4f}, {lon:.4f}: {count} sensors")
                
        # Check for vertical lines (same longitude)
        lon_counts = lon_rounded.value_counts()
        vertical_lines = lon_counts[lon_counts > 20]
        
        if len(vertical_lines) > 0:
            print("⚠️  Potential vertical line artifacts (>20 sensors at same longitude):")
            for lon, count in vertical_lines.head().items():
                print(f"   Longitude {lon:.3f}: {count} sensors")
                
        # Check coordinate sources
        source_counts = df['Coordinate_Source'].value_counts()
        print(f"\nCoordinate extraction sources:")
        for source, count in source_counts.items():
            print(f"   {source}: {count} sensors")
            
        # Geographic distribution
        print(f"\nGeographic distribution:")
        print(f"   Latitude range: {df['Latitude'].min():.4f} to {df['Latitude'].max():.4f}")
        print(f"   Longitude range: {df['Longitude'].min():.4f} to {df['Longitude'].max():.4f}")
        
    def create_shapefile(self, output_filename="ismn_sensors_improved.shp"):
        """Create shapefile with quality filtering"""
        if not self.sensors_data:
            raise ValueError("No sensor data found")
            
        df = pd.DataFrame(self.sensors_data)
        
        # Quality filtering
        print("Applying quality filters...")
        original_count = len(df)
        
        # Remove obvious errors
        df = df[df['Latitude'] != 0]  # Remove null island
        df = df[df['Longitude'] != 0]
        df = df[df['Latitude'] != df['Longitude']]  # Remove same lat/lon
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['Sensor_ID', 'Station_ID', 'Network_ID', 'Latitude', 'Longitude'])
        
        print(f"Filtered {original_count - len(df)} problematic records")
        
        # Create geometry
        geometry = [Point(xy) for xy in zip(df['Longitude'], df['Latitude'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
        
        # Save outputs
        output_path = self.output_dir / output_filename
        gdf.to_file(output_path)
        
        csv_path = self.output_dir / f"{output_filename.replace('.shp', '.csv')}"
        df.to_csv(csv_path, index=False)
        
        # Save debug info
        if self.debug_info:
            debug_path = self.output_dir / "debug_failed_coordinates.txt"
            with open(debug_path, 'w') as f:
                for info in self.debug_info:
                    f.write(f"File: {info['file']}\n")
                    f.write(f"Issue: {info['issue']}\n")
                    f.write("First 10 lines:\n")
                    for line in info['first_10_lines']:
                        f.write(f"  {line}\n")
                    f.write("-" * 50 + "\n")
        
        print(f"\nShapefile created: {output_path}")
        print(f"Total sensors: {len(gdf)}")
        print(f"Networks: {gdf['Network_Name'].nunique()}")
        print(f"Stations: {gdf['Station_ID'].nunique()}")
        print(f"CSV saved: {csv_path}")
        
        return gdf

def process_ismn_improved(zip_path, output_dir="output", shapefile_name="ismn_sensors_improved.shp"):
    """
    Process ISMN data with improved coordinate extraction
    """
    processor = ISMNImprovedProcessor(zip_path, output_dir)
    processor.process_zip_directly()
    gdf = processor.create_shapefile(shapefile_name)
    
    return gdf

# Example usage:
"""
zip_file = "your_ismn_data.zip"
gdf = process_ismn_improved(zip_file, "output", "sensors_improved.shp")
print(gdf.head())

# Plot with better visualization
import matplotlib.pyplot as plt
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
gdf.plot(ax=ax1, markersize=0.5, alpha=0.7)
ax1.set_title("All Sensors")
ax1.set_xlabel("Longitude")
ax1.set_ylabel("Latitude")

# Plot longitude histogram to check for vertical lines
gdf['Longitude'].hist(bins=100, ax=ax2)
ax2.set_title("Longitude Distribution")
ax2.set_xlabel("Longitude")
ax2.set_ylabel("Count")
plt.tight_layout()
plt.show()
"""