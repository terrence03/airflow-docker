"""
This script synchronizes files from a source folder to a destination folder.
Functions:
    sync_folders(source_folder, destination_folder):
        Recursively copies files and directories from the source folder to the destination folder.
        Only copies files if they do not exist in the destination folder or if they have been modified more recently in the source folder.
Usage:
    Run the script with the source and destination folder paths specified in the main block.
Example:
    python sync_gas_station_namelist.py
Notes:
    - Ensure that the source and destination folder paths are correctly specified.
    - The script will create the destination folder if it does not exist.
    - The script uses `shutil.copy2` to preserve file metadata during the copy process.
"""

import os
import shutil


def sync_folders(source_folder, destination_folder):
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for root, dirs, files in os.walk(source_folder):
        relative_path = os.path.relpath(root, source_folder)
        destination_root = os.path.join(destination_folder, relative_path)

        if not os.path.exists(destination_root):
            os.makedirs(destination_root)

        for file in files:
            source_file = os.path.join(root, file)
            destination_file = os.path.join(destination_root, file)

            if not os.path.exists(destination_file) or os.path.getmtime(
                source_file
            ) > os.path.getmtime(destination_file):
                shutil.copy2(source_file, destination_file)


if __name__ == "__main__":
    source_folder = R"D:\Projects\airflow-docker\downloads\gas_station_namelist"
    destination_folder = R"Y:\0  資料庫\0  自動更新資料\1. 加油站名冊_每日"
    sync_folders(source_folder, destination_folder)
    print("Sync complete.")
