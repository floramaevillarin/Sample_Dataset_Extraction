Sample Dataset Extraction

This process allows you to extract a sample from the dataset stored in Azure Blob Storage. By running a simple command, you can obtain a specified number of samples and save them locally. Below are the steps and a sample output to guide you through the process.

To extract a sample from the dataset, you can run the following command in the terminal. This example demonstrates how to obtain 10 samples from the dataset:
> python main.py 10

Sample Output:
> $ python main.py 10<br/>
Credentials loaded successfully <br/>
Connected to Azure Blob Storage successfully<br/>
big_data.csv already exists.<br/>
big_rating.csv already exists.<br/>
Files merged successfully. Shape: (1000, 10)<br/>
Sample of 10 rows obtained successfully.<br/>
Sample Dataset saved to sample_dataset_10_20240726_095929.csv<br/>
0.01s - Debugger warning: It seems that frozen modules are being used, which may<br/>
....<br/>
Notebook  executed successfully.<br/>
Preprocessing notebook executed successfully. Now running feature extraction notebook...<br/>
...<br/>
Notebook feature-extraction.ipynb executed successfully.<br/>
Feature extraction notebook executed successfully.<br/>
Connection to Azure Blob Storage closed...<br/>
