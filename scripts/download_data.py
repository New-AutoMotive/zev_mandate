#!/usr/bin/env python3
from data_downloader import UKCarDataDownloader

if __name__ == "__main__":
    print("Starting UK car data download...")
    downloader = UKCarDataDownloader()
    
    try:
        # Download all datasets
        downloader.get_latest_fuel_consumption_data()
        downloader.get_vehicle_statistics()
        downloader.get_traffic_data()
        downloader.get_emissions_data()
        
        print("\nAll data downloaded successfully!")
        print("\nYou can now run the analysis notebook: uk_car_emissions_analysis.ipynb")
        
    except Exception as e:
        print(f"\nError downloading data: {str(e)}")
        raise
