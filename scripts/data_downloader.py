#!/usr/bin/env python3
import requests
import pandas as pd
import os
from pathlib import Path
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UKCarDataDownloader:
    def __init__(self):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

    def download_file(self, url, filename):
        """Generic file downloader with progress tracking"""
        logger.info(f"Downloading {filename} from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        file_path = self.data_dir / filename
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded {filename}")
        return file_path

    def get_latest_fuel_consumption_data(self):
        """Download latest road transport fuel consumption data"""
        base_url = "https://www.gov.uk/government/collections/road-transport-consumption-at-regional-and-local-level"
        
        # Get the collection page
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the first script tag with application/ld+json
        script = soup.find('script', type='application/ld+json')
        if not script:
            raise Exception("Could not find JSON data in page")
        
        # Parse the JSON data
        import json
        data = json.loads(script.string)
        
        # Get the latest publication from hasPart
        has_part = data.get('hasPart', [])
        if not has_part:
            raise Exception("No publications found in JSON data")
            
        # Get the first (latest) publication
        latest_pub = has_part[0]
        pub_url = latest_pub.get('sameAs', '')
        if not pub_url:
            raise Exception("Could not find publication URL")
            
        # Get the publication page
        response = requests.get(pub_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the ODS file download link
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('.ods'):
                if not href.startswith('http'):
                    href = 'https://www.gov.uk' + href
                return self.download_file(href, 'road-transport-fuel-consumption-tables.ods')
        
        raise Exception("Could not find ODS file download link")

    def _get_vehicle_stats_url(self, page_url, table_id):
        """Get download URL for a specific vehicle statistics table"""
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the script tag with application/ld+json
        script = soup.find('script', type='application/ld+json')
        if not script:
            raise Exception(f"Could not find JSON data for {table_id}")
            
        # Parse the JSON data
        import json
        data = json.loads(script.string)
        
        # Look for the download URL in the JSON
        for attachment in data.get('distribution', []):
            if table_id.lower() in attachment.get('name', '').lower():
                url = attachment.get('contentUrl', '')
                if url:
                    if not url.startswith('http'):
                        url = 'https://www.gov.uk' + url
                    return url
                    
        raise Exception(f"Could not find download URL for {table_id}")

    def get_vehicle_statistics(self):
        """Download vehicle licensing statistics VEH0105 and VEH0142"""
        base_url = "https://www.gov.uk/government/statistical-data-sets/all-vehicles-veh01"
        
        # Get VEH0105
        try:
            veh0105_url = self._get_vehicle_stats_url(base_url, 'VEH0105')
            self.download_file(veh0105_url, "veh0105.ods")
        except Exception as e:
            logger.error(f"Error downloading VEH0105: {str(e)}")
            raise
            
        # Get VEH0142 from a different page
        try:
            veh0142_url = self._get_vehicle_stats_url(
                "https://www.gov.uk/government/statistical-data-sets/ultra-low-emission-vehicles-ulev",
                'VEH0142'
            )
            self.download_file(veh0142_url, "veh0142.ods")
        except Exception as e:
            logger.error(f"Error downloading VEH0142: {str(e)}")
            raise

    def get_traffic_data(self):
        """Download traffic data from DfT API"""
        try:
            # Get GB-wide data
            url = 'https://roadtraffic.dft.gov.uk/api/traffic/regions/vehicle-type?filter[region_id]=4'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()['data']
            
            df = pd.DataFrame(data)
            df.to_csv(self.data_dir / 'traffic_data.csv', index=False)
            logger.info("Downloaded traffic data")
        except Exception as e:
            logger.error(f"Error downloading traffic data: {str(e)}")
            raise

    def get_emissions_data(self):
        """Download latest NAEI emissions data"""
        # This would normally require manual download from NAEI website
        # For now, we'll just check if the file exists
        emissions_file = self.data_dir / "2306200930_DA_GHGI_1990-2021_Final_v3.1.xlsx"
        if not emissions_file.exists():
            logger.warning(
                "Emissions data file not found. Please download manually from: "
                "https://naei.beis.gov.uk/data/data-selector"
            )

def main():
    downloader = UKCarDataDownloader()
    
    try:
        # Download all datasets
        downloader.get_latest_fuel_consumption_data()
        downloader.get_vehicle_statistics()
        downloader.get_traffic_data()
        downloader.get_emissions_data()
        
        logger.info("All data downloaded successfully!")
        
    except Exception as e:
        logger.error(f"Error downloading data: {str(e)}")
        raise

if __name__ == "__main__":
    main()
