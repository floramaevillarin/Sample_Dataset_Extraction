import subprocess
import os

from sampledataset import SampleDataset
from azureconection import AzureConnection
from filereader import FileReader
from datetime import datetime

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

'''
Included in requirements.txt are the libraries that need to be installed:
If you are using Python 3 and the pip command defaults to Python 2, use pip3 instead:
    > pip install azure-storage-blob 
    > pip3 install azure-storage-blob
To verify:
    > pip show azure-storage-blob
If you encounter permission issues when installing the package:
    > pip install --user azure-storage-blob
Using jupyter to run the ipynb file:
    > pip install jupyter or pip install --upgrade jupyter nbconvert
To execute Jupyter notebooks without converting them to another format:
    > pip install nbclient

To get the 10 samples from dataset, in terminal run:"
> python main.py 10

Sample Output:
$ python main.py 10
Credentials loaded successfully
Connected to Azure Blob Storage successfully
BigData.csv already exists.
Files merged successfully. Shape: (2000000, 10)
Sample of 10 rows obtained successfully.
Sample Dataset saved to sample_dataset_10_20240626_095929.csv
0.01s - Debugger warning: It seems that frozen modules are being used, which may
0.00s - make the debugger miss breakpoints. Please pass -Xfrozen_modules=off
...
Notebook executed successfully.
Feature extraction notebook executed successfully.
Connection to Azure Blob Storage closed...
'''

# To execute Jupyter notebooks without converting them to another format
def run_notebook(notebook_path, filename=None):
    try:
        with open(notebook_path) as f:
            nb = nbformat.read(f, as_version=4)
        
        if filename:
            os.environ["SAMPLE_DATA_FILENAME"] = filename
        
        client = NotebookClient(nb)
        client.execute()

        print(f"Notebook {notebook_path} executed successfully.")
        return True
    except CellExecutionError as e:
        print(f"Error executing the notebook {notebook_path}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    
def main(num_sample):
    credentials_file = "azure_credentials.pkl"
    
    # Replace with your container name and file name
    container_name = "BigDataSets" 
    big_data_file = "big_data.csv"  
    big_rating_file = "big_rating.csv" 
    base_dir = "datasets"

    # Ensure the base directory exists
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # Define paths for the files
    big_data_file_path = os.path.join(base_dir, big_data_file)
    big_rating_file_path = os.path.join(base_dir, big_rating_file)

    # Create an instance of AzureConnection, load credentials and connect
    azure_connection = AzureConnection(credentials_file)
    azure_connection.load_credentials()
    azure_connection.connect()
    azure_connection.list_blobs(container_name)

    # Create an instance of FileReader
    file_reader = FileReader(azure_connection)   

    # Check if books_data_file exist before downloading
    if not os.path.exists(big_data_file_path):
        print(f"Downloading {big_data_file} start...")
        file_reader.download_blob_to_file(container_name, big_data_file)    
    else:
        print(f"{big_data_file} already exists.")
    
    # Check if big_rating_file exist before downloading
    if not os.path.exists(big_rating_file_path):
        print(f"Downloading {big_rating_file} start...")
        file_reader.download_blob_to_file(container_name, big_rating_file) 
    else:
        print(f"{big_rating_file} already exists.")

    # Check if files were downloaded successfully
    if os.path.exists(big_data_file_path) and os.path.exists(big_rating_file_path):

        # Create an instance of SampleDataset and merge files
        sample_dataset = SampleDataset(big_data_file_path, big_rating_file_path)
        sample_dataset.merge_files()

        # Generate filename with current date and time
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'sample_dataset_{num_sample}_{timestamp}.csv'
        
        # Get a sample of the merged data
        sample_df = sample_dataset.get_sample(num_samples=num_sample)

        if sample_df is not None:
            file_reader.save_dataframe(sample_df, filename)
            
            # Run the preprocessing notebook
            if run_notebook('data-preprocessing.ipynb', filename=filename):
                print("Preprocessing notebook executed successfully. Now running feature extraction notebook...")
                # Run the feature extraction notebook
                if run_notebook('feature-extraction.ipynb'):
                    print("Feature extraction notebook executed successfully.")
                    # Run the feature engineering notebook
                    if run_notebook('feature_engineering.ipynb'):
                        print("Feature Engineering notebook executed successfully.")
                    else:
                        print("Failed to execute feature engineering notebook.")
                else:
                    print("Failed to execute feature extraction notebook.")
            else:
                print("Failed to execute preprocessing notebook.")                    
        else:
            print("Failed to create sample. Exiting...")
    
    # Close the Azure connection
    azure_connection.close()

if __name__ == "__main__":
    import argparse
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Process Azure Blob data and get a sample...')
    parser.add_argument('num_samples', type=int, help='Number of samples to retrieve from the merged data...')

    args = parser.parse_args()
    main(args.num_samples)