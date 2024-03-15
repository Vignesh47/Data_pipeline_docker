
# My project

The given Input data has to loaded into Spark Dataframe and transformed into cleaned dataframe based on the use case and stored in csv Format
1. Import the required Library
2. Ingestion.py file will read the input file from data folder and convert the file into dataframe
3. Using Transformation.py file the data will be processed and transformed into clean_data
4. Storage.py will store the data into csv format along with datetime
5. Unittesting is performed for the multiple scenarios

### Steps to run
run the following to install the dependencies.
```commandline
pip install -r requirements.txt
```
```commandline
python main.py -> for main function
python test_integration.py for unittesting
```


