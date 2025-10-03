
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