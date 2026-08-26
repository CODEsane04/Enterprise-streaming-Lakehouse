import kagglehub
import shutil
import os

def download_olist_data():
    print("Downloading Olist dataset from Kaggle...")
    # Download latest version of the dataset
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    
    print(f"Dataset downloaded to: {path}")
    
    # Destination folder
    dest = r"d:\DE project\data\olist"
    os.makedirs(dest, exist_ok=True)
    
    # Copy all CSV files
    print(f"Copying CSV files to {dest}...")
    copied_files = 0
    for file in os.listdir(path):
        if file.endswith('.csv'):
            source_file = os.path.join(path, file)
            dest_file = os.path.join(dest, file)
            shutil.copy2(source_file, dest_file)
            print(f"  - Copied {file}")
            copied_files += 1
            
    print(f"✅ Success! Copied {copied_files} CSV files to {dest}")

if __name__ == "__main__":
    download_olist_data()
