
# Python

Poetry is used for depedency management, ensure poetry is installed globally.

```sh
pip install poetry
```

Ensure you relevant settings are set up in `.env`, note .env is in the gitignore to prevent leakage of keys etc. If a new key is added and a placeholder value is neccessary manually run `git add .env` and ensure the data is free from any sensitive keys.

To install all dependencies run

```sh
poetry install
```
