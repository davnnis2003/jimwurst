import csv
import os
import holidays
from datetime import date
import inspect

# Configuration
# Subdivisions to include for specific countries
SUBDIVISIONS_CONFIG = {
    'DE': ['BE'], # Berlin
    # Add others here as needed, e.g., 'US': ['NY', 'CA']
}

# Years to generate
START_YEAR = 2020
END_YEAR = 2030
YEARS = range(START_YEAR, END_YEAR + 1)

# Output path (dbt seeds)
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data_transformation/dbt/seeds"))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "public_holidays.csv")

def get_all_supported_countries():
    """Dynamically fetch all supported country codes from the holidays library."""
    supported_countries = []
    
    # In holidays v0.83+, country attributes are EntityLoader instances, not classes.
    # We iterate dir() and filter for 2-letter uppercase codes.
    for name in dir(holidays):
        # We only care about potential country codes (uppercase, length 2)
        if len(name) == 2 and name.isupper() and name != 'ZE':
            supported_countries.append(name)
                
    return sorted(list(set(supported_countries)))

def generate_holidays():
    # 1. Identify all target countries
    target_countries = get_all_supported_countries()
    print(f"Generating holidays for {len(target_countries)} countries from {START_YEAR} to {END_YEAR}...")
    
    # List to store all holiday records
    holiday_records = []

    for country_code in target_countries:
        try:
            # --- A. National Holidays ---
            # Fetch national holidays (subdiv=None)
            national_holidays_obj = holidays.country_holidays(country_code, years=YEARS)
            
            # Convert to a dict for easy lookup later (date -> name)
            # This handles the "observed" logic implicitly if the library supports it, 
            # usually the keys are the actual holiday dates.
            national_holidays_map = {d: name for d, name in national_holidays_obj.items()}

            for h_date, h_name in national_holidays_map.items():
                holiday_records.append({
                    'date': h_date,
                    'country_code': country_code,
                    'subdivision_code': '', # Empty for National
                    'holiday_name': h_name
                })

            # --- B. Subdivision Holidays ---
            # Check if we have specific subdivisions requested for this country
            if country_code in SUBDIVISIONS_CONFIG:
                for subdiv_code in SUBDIVISIONS_CONFIG[country_code]:
                    try:
                        subdiv_holidays_obj = holidays.country_holidays(country_code, subdiv=subdiv_code, years=YEARS)
                        
                        for h_date, h_name in subdiv_holidays_obj.items():
                            # Deduplication: Only add if NOT in national holidays for this date
                            # Or if the name is significantly different? 
                            # Usually strict date exclusion is safer to avoid duplicates.
                            # Determining if a holiday is "state specific" usually means it exists in state but not national.
                            
                            if h_date not in national_holidays_map:
                                holiday_records.append({
                                    'date': h_date,
                                    'country_code': country_code,
                                    'subdivision_code': subdiv_code,
                                    'holiday_name': h_name
                                })
                    except NotImplementedError:
                         print(f"Warning: Subdivisions not implemented for {country_code}")
                    except Exception as e:
                        print(f"Error fetching subdivision {subdiv_code} for {country_code}: {e}")

        except Exception as e:
            print(f"Skipping {country_code} due to error: {e}")
    
    # Sort by date, country, subdivision
    holiday_records.sort(key=lambda x: (x['date'], x['country_code'], x['subdivision_code']))
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Write to CSV
    print(f"Writing to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'country_code', 'subdivision_code', 'holiday_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for record in holiday_records:
            writer.writerow(record)
            
    print(f"Successfully generated {len(holiday_records)} holiday records.")

if __name__ == "__main__":
    generate_holidays()
