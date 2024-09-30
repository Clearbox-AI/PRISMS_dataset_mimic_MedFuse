import pandas as pd
import asyncio
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import subprocess
import argparse
from typing import List, Set

def download_patient_data(patient_id: str, wget_path: Path, download_directory: Path, username: str, password: str) -> None:
    """
    Downloads data for a given patient from PhysioNet.

    Args:
        patient_id: The ID of the patient to download data for.
        wget_path: The path to the wget executable.
        download_directory: The directory where the downloaded data should be saved.
        username: The PhysioNet username.
        password: The PhysioNet password.
    """
    
    try:
        folder = f"p{patient_id[:2]}"  # Derive the folder from the first two digits
        patient_folder = f"p{patient_id}"  # Derive the patient-specific folder

        # Construct the URL
        url = f"https://physionet.org/files/mimic-cxr-jpg/2.0.0/files/{folder}/{patient_folder}/"
        print(f"Downloading data for patient {patient_id} from {url}")

        # Construct the wget command
        command = [
            str(wget_path.resolve()),
            "-r", "-N", "-c", "-np", "-P",
            str(download_directory.resolve()),
            "--user", username,
            "--password", password,
            url
        ]

        # Run the wget command
        subprocess.run(command, check=True)

    except Exception as e:
        print(f"An error occurred while downloading for patient {patient_id}: {e}")

async def download_all_data(mimic_iv_csv_path: Path, wget_path: Path, download_directory: Path, username: str, password: str, min_workers: int) -> None:
    """
    Downloads data for all patients in the given CSV file.

    Args:
        mimic_iv_csv_path: The path to the CSV file containing the patient IDs.
        wget_path: The path to the wget executable.
        download_directory: The directory where the downloaded data should be saved.
        username: The PhysioNet username.
        password: The PhysioNet password.
        min_workers: The minimum number of workers to use. The actual number of workers will be the minimum of this value and the number of patients.

    Returns:
        None
    """

    try:
        # Step 1: Read the CSV file and extract subject_id column
        df = pd.read_csv(mimic_iv_csv_path)
        patient_ids = set(df['subject_id'].astype(str).tolist())  # Convert to string set

        # Step 2: For each patient, construct the wget command and run it
        with ThreadPoolExecutor(max_workers=min(min_workers, len(patient_ids))) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, download_patient_data, patient_id, wget_path, download_directory, username, password) for patient_id in patient_ids if patient_id.isdigit()]
            await asyncio.gather(*tasks)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":

    # Define the command-line arguments
    parser = argparse.ArgumentParser(description="Download data for MIMIC-IV patients")
    parser.add_argument("--mimic_iv_csv_path", type=Path, required=True, help="Path to the CSV file")
    parser.add_argument("--wget_path", type=Path, required=True, help="Path to the wget executable")
    parser.add_argument("--download_directory", type=Path, required=True, help="Path to local download directory")
    parser.add_argument("--username", type=str, required=True, help="PhysioNet username")
    parser.add_argument("--password", type=str, required=True, help="PhysioNet password")
    parser.add_argument("--min_workers", type=int, default=20, help="Minimum number of concurrent tasks")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the function to download data
    asyncio.run(download_all_data(args.mimic_iv_csv_path, args.wget_path, args.download_directory, args.username, args.password, args.min_workers))