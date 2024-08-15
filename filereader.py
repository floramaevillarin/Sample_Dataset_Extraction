from io import StringIO
import pandas as pd
import os

class FileReader:
    def __init__(self, azure_connection):
        self.azure_connection = azure_connection

    def read_csv(self, container_name, file_name):
        if self.azure_connection.blob_service_client:
            try:
                container_client = self.azure_connection.blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(file_name)
                
                # Download the blob data
                blob_data = blob_client.download_blob().readall()
                
                # Read the data 
                csv_data = StringIO(blob_data.decode('utf-8'))
                df = pd.read_csv(csv_data, on_bad_lines='skip',  
                                            encoding='utf-8',
                                            engine='python', quotechar='"', escapechar='\\')
                
                print("CSV file read into DataFrame successfully")
                return df
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                return None
        else:
            print("Blob service client is not connected. Please call connect() first.")
            return None
        
    def download_blob_to_file(self, container_name, file_name):
        if self.azure_connection.blob_service_client:
            try:
                container_client = self.azure_connection.blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(file_name)

                # Create the datasets directory if it doesn't exist
                os.makedirs('datasets', exist_ok=True)
                local_path = os.path.join('datasets', file_name)

                # Download the blob content
                blob_content = blob_client.download_blob().readall()

                # Save the blob content to a local file
                with open(local_path, "wb") as file:
                    print(f"Downloading {file_name} start...")
                    file.write(blob_content)
                print(f"Downloading {file_name} successfully...")

            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            print("Blob service client is not connected...")

    # Save the dataframe to a file
    def save_dataframe(self, df, filename):
        try:
            datasets_dir = "datasets"
            file_path = os.path.join(datasets_dir, filename)
            df.to_csv(file_path, index=False)
            print(f"Sample Dataset saved to {filename}")
        except Exception as e:
            print(f"Error saving sample: {e}")
