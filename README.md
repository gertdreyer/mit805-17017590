NYC Citi Bike Data Analysis
======================
This repository contains code to process and analyse the NYC Citi Bike data. The data is available from https://www.citibikenyc.com/system-data
The data is in CSV format and contains information about bike trips, including start and end times, trip duration, start and end stations, and user types.
The code uses Hadoop Streaming to process the data in a distributed manner. The mapper and reducer scripts are written in Python.
The output of the Hadoop job is a CSV file containing aggregated data about bike trips, including total trips, average trip duration, and user type breakdowns.
The output is then converted to parquet format for further analysis.

Installation
============

# Python

Poetry is used for depedency management, ensure poetry is installed globally.

```sh
pip install poetry
```

Ensure you relevant settings are set up in `.env`, note .env is in the gitignore to prevent leakage of keys etc. If a new key is added and a placeholder value is neccessary manually run `git add .env` and ensure the data is free from any sensitive keys.

Requires a system installed python 3.12 executable

To install all dependencies run

```sh
poetry env use 3.12
poetry lock
poetry sync
```

```sh
$(poetry env activate)
```

To run the Hadoop project with the local runner use the below command
```sh
python ./map_reduce.py -r local --local-tmp-dir temp  ../data/*.csv -o out
```

The results will be in `out`. 

After the hadoop run it is advisable to convert to parquet for further analysis

```sh
python post_processing.py
```

this will convert all csv files in `out` to parquet files in `p/out` and at the same time flatten the nested structures of bike flows. 

This can then be used with the interactive analysis notebook `analysis.ipynb` which uses various python libraries to process the aggregated data and visualise it. 

Google colab is the best approach to run this notebook as it has all the relevant libraries pre-installed and free GPU usage.