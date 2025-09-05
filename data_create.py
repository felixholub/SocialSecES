import numpy as np
import pandas as pd
import os
import re
from pathlib import Path

# Configuration
BASE_DIR = Path("C:/Users/holub/Data/afiliados")  # Set your project folder here
DATA_DIR = BASE_DIR / "src_data"
OUTPUT_DIR = BASE_DIR / "out_data"


def get_date_from_filename(filename):
    """
    Extract year and month from filename format 'AfiliadosMuni-MM-YYYY*'
    Handles variations like:
    - AfiliadosMuni-01-2010_late_data.xlsx
    - AfiliadosMuni-01-2012+DEFINITIVO+mod_late_data.xlsx
    - AfiliadosMuni-03-2005.xlsx
    """
    # Use regex to extract the month-year pattern
    match = re.search(r"AfiliadosMuni-(\d{2})-(\d{4})", filename)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        return year, month
    else:
        raise ValueError(f"Could not extract date from filename: {filename}")


def extract_municipality_codes(municipality_series):
    """Extract municipality codes from municipality names"""
    # Filter out null values and distribution placeholders
    valid_munis = municipality_series.dropna()
    valid_munis = valid_munis[valid_munis != "SIN DISTRIBUCIÓN (*)"]

    # Extract the first number from each municipality name
    codes = [int(name.split()[0]) for name in valid_munis]
    return codes


def load_and_combine_data():
    """Load all Excel files and combine them into a single DataFrame"""
    excel_files = list(DATA_DIR.glob("*.xlsx"))
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {DATA_DIR}")

    # Process all files
    data_frames = []
    for file_path in excel_files:
        try:
            print(f"Processing: {file_path.name}")

            # Load data
            data = pd.read_excel(file_path, header=1)

            # Remove rows with all missing values
            data = data.dropna(how="all")

            # Remove rows where "PROVINCIA" starts with an opening parenthesis
            data = data[~data["PROVINCIA"].astype(str).str.startswith("(")]

            # Remove rows where "MUNICIPIO" starts with "SIN DISTRIBUCIÓN"
            data = data[
                ~data["MUNICIPIO"].astype(str).str.startswith("SIN DISTRIBUCIÓN")
            ]

            # Extract date from filename
            year, month = get_date_from_filename(file_path.name)
            data["year"] = year
            data["month"] = month

            column_renames = {
                "Reg. General(1)": "GENERAL",
                "TRAB.": "TRAB",
                "R. G.- S.E.Agrario": "AGRARIO",
                "R.E.MAR": "MAR",
                "HOGAR (2)": "HOGAR",
                "R.E.Autónomos": "AUTONOMOS",
                "R.E. Carbón": "CARBON",
                "R. G.- S.E.Hogar(2)": "HOGAR",
                "R. E. MAR": "MAR",
                "R. E. T. Autónomos": "AUTONOMOS",
                "R. E. M. Carbón": "CARBON",
                "R. G.- S.E.Hogar": "HOGAR",
                "R. E. MAR ": "MAR",
            }
            data = data.rename(columns=column_renames)

            data_frames.append(data)
            print(f"  → Extracted date: {month}/{year}")

        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
            continue

    if not data_frames:
        raise ValueError("No valid data files were processed")

    # Combine all data
    combined_data = pd.concat(data_frames, ignore_index=True)

    # Save complete dataset
    OUTPUT_DIR.mkdir(exist_ok=True)
    combined_data.to_csv(OUTPUT_DIR / "all_data.csv", index=False)

    return combined_data


def clean_numeric_columns(data):
    """Clean all numeric columns by handling special values"""
    data = data.copy()

    # List of columns that should be numeric
    numeric_columns = [
        "GENERAL",
        "AGRARIO",
        "MAR",
        "HOGAR",
        "AUTONOMOS",
        "CARBON",
        "TOTAL",
    ]

    for column in numeric_columns:
        if column in data.columns:
            # Convert to string first to ensure consistent replacement
            data[column] = data[column].astype(str)

            # Replace "<5" with NaN
            data.loc[data[column] == "<5", column] = np.nan

            # Convert to numeric, coercing any non-numeric values to NaN
            data[column] = pd.to_numeric(data[column], errors="coerce")

    return data


def determine_afiliados(data):
    """Create a single measure of general afiliados"""
    data = data.copy()

    # Before 2012, use GENERAL; from 2012 onwards, use GENERAL + HOGAR if HOGAR is not NA
    data["AFILIADOS"] = np.where(
        (data["year"] < 2012) | (data["HOGAR"].isna()),
        data["GENERAL"],
        data["GENERAL"] + data["HOGAR"],
    )
    data = data[["PROVINCIA", "MUNICIPIO", "year", "month", "AFILIADOS"]]
    return data

def create_municipality_averages(data):
    """Create municipality-level averages by year"""
    # Filter out invalid municipalities
    muni_data = data.dropna(subset=["MUNICIPIO"])
    muni_data = muni_data[muni_data["MUNICIPIO"] != "PROVINCIAL"]

    # Extract municipality codes
    muni_data["MUNI_CODE"] = extract_municipality_codes(muni_data["MUNICIPIO"])

    # Calculate averages by municipality and year
    averages = (
        muni_data.groupby(["MUNI_CODE", "year"])["AFILIADOS"].mean().reset_index()
    )

    return averages


def create_national_averages(data):
    """Create national-level averages by year"""
    # Filter for national data
    national_data = data[data["PROVINCIA"] == "NACIONAL"]
    national_data = national_data[["PROVINCIA", "AFILIADOS", "year", "month"]]

    # Calculate averages by year
    averages = national_data.groupby("year")["AFILIADOS"].mean().reset_index()
    averages["PROVINCIA"] = "NACIONAL"

    return averages


def create_provincial_averages(data):
    """Create provincial-level averages by year"""
    # Filter out provincial data"""
    provincial_data = data[data["MUNICIPIO"] == "PROVINCIAL"]
    provincial_data = provincial_data[["PROVINCIA", "AFILIADOS", "year", "month"]]

    # Calculate averages by province and year
    averages = (
        provincial_data.groupby(["PROVINCIA", "year"])["AFILIADOS"].mean().reset_index()
    )
    return averages


def main():
    """Main processing function"""
    print(f"Working with data in: {BASE_DIR}")
    print("Starting data processing...")

    # Always process all files
    print("Combining source files...")
    data = load_and_combine_data()

    # Clean the target column
    print("Cleaning data...")
    data = clean_numeric_columns(data)
    data = determine_afiliados(data)

    # Create municipality averages
    print("Creating municipality averages...")
    muni_averages = create_municipality_averages(data)
    muni_averages.to_csv(OUTPUT_DIR / "averages_muni.csv", index=False)

    # Create national averages
    print("Creating national averages...")
    national_averages = create_national_averages(data)
    national_averages.to_csv(OUTPUT_DIR / "averages_nacional.csv", index=False)

    # Create provincial averages
    print("Creating provincial averages...")
    provincial_averages = create_provincial_averages(data)
    provincial_averages.to_csv(OUTPUT_DIR / "averages_provincial.csv", index=False)

    print("Processing complete!")
    print(f"Municipality averages: {len(muni_averages)} rows")
    print(f"National averages: {len(national_averages)} rows")
    print(f"Provincial averages: {len(provincial_averages)} rows")


if __name__ == "__main__":
    main()
