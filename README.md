Sample Dataset Extraction

This process allows you to extract a sample from the dataset stored in Azure Blob Storage. By running a simple command, you can obtain a specified number of samples and save them locally. Below are the steps and a sample output to guide you through the process.

To extract a sample from the dataset, you can run the following command in the terminal. This example demonstrates how to obtain 10 samples from the dataset:
> python main.py 10

Sample Output:
> $ python main.py 10
Credentials loaded successfully <br/>
Connected to Azure Blob Storage successfully
big_data.csv already exists.
big_rating.csv already exists.
Files merged successfully. Shape: (1000, 10)
Sample of 10 rows obtained successfully.
Sample Dataset saved to sample_dataset_10_20240726_095929.csv
0.01s - Debugger warning: It seems that frozen modules are being used, which may
....
Notebook  executed successfully.
Preprocessing notebook executed successfully. Now running feature extraction notebook...
...
Notebook feature-extraction.ipynb executed successfully.
Feature extraction notebook executed successfully.
Connection to Azure Blob Storage closed...
