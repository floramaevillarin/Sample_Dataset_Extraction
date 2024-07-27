import pandas as pd

class SampleDataset:
    def __init__(self, big_data_file, big_rating_file):
        self.big_data_file = big_data_file
        self.big_rating_file = big_rating_file
        self.merged_df = None

    def merge_files(self):
        try:
            big_data_df = pd.read_csv(self.big_data_file)
            big_rating_df = pd.read_csv(self.big_rating_file)
            
            # Perform an inner join on the 'id' column
            self.merged_df = pd.merge(big_data_df, big_rating_df, on='id', how='outer', validate="one_to_many")
            print(f"Files merged successfully. Shape: {self.merged_df.shape}")
        except Exception as e:
            print(f"Error merging files: {e}")

    def filter_data(self, start_date, end_date):
        if self.merged_df is not None:
            try:
                # Convert the published_date column to datetime
                self.merged_df['publishedDate'] = pd.to_datetime(self.merged_df['publishedDate'], errors='coerce')
                
                # Filter the DataFrame
                filtered_df = self.merged_df.loc[(self.merged_df['publishedDate'] >= start_date) & 
                                                 (self.merged_df['publishedDate'] <= end_date)]
                print(f"Data filtered for the range {start_date} to {end_date}.")
                return filtered_df
            except Exception as e:
                print(f"Error filtering data: {e}")
                return None
        else:
            print("Merged DataFrame is not available...")
            return None

    # Filter the data for the years between 2010 and 2024 
    def get_sample(self, num_samples, start_date='2004-01-01', end_date='2024-07-04'):
        filtered_df = self.filter_data(start_date, end_date)
        if filtered_df is not None:
            try:
                sample_df = filtered_df.sample(n=num_samples)
                print(f"Sample of {num_samples} rows obtained successfully.")
                return sample_df
            except Exception as e:
                print(f"Error getting sample: {e}")
                return None
        else:
            print("Filtered DataFrame is not available...")
            return None
