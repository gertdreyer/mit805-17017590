from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
import holidays
from datetime import datetime
from tqdm import tqdm

import timedelta


ny_holidays = holidays.US(state="NY")


def line_processor(line: str) -> dict:
    index, data = line.split("\t", 1)
    year, month, day, hour, station = json.loads(index)
    data = json.loads(data)
    total_trips, average_duration, bikes_in, bikes_out = data
    bikes_in = {f"bikes_in_{k}": v for k, v in bikes_in.items()}
    bikes_out = {f"bikes_out_{k}": v for k, v in bikes_out.items()}
    return {
        "date": f"{year}-{month:02d}-{day:02d} {hour:02d}:00:00",
        "station": station,
        "total_trips": total_trips,
        "average_duration": average_duration,
        **bikes_in,
        **bikes_out,
        "is_holiday": int(
            pd.to_datetime(f"{year}-{month:02d}-{day:02d}").date() in ny_holidays
        ),
        "is_weekend": int(
            pd.to_datetime(f"{year}-{month:02d}-{day:02d}").date().weekday() >= 5
        ),
    }


all_files = []


def single_file_to_dataframe(file):
    with open(file, "r") as f:
        lines = f.readlines()
    with ProcessPoolExecutor() as executor:
        data = list(
            tqdm(
                executor.map(line_processor, lines),
                total=len(lines),
                desc=f"Processing {file}",
                position=1,
            )
        )
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    store_dataframe_to_parquet(df, "p/" + file + ".parquet")


def load_data_to_parquet(directory):
    all_files = [os.path.join(directory, f) for f in os.listdir(directory)]
    for file in tqdm(all_files):
        if not os.path.exists("p"):
            os.makedirs("p")
        single_file_to_dataframe(file)


def store_dataframe_to_parquet(df, filename):
    df.to_parquet(filename, index=False)


def read_dataframe_from_parquet(filename):
    return pd.read_parquet(filename)


if __name__ == "__main__":
    # Load data from JSON files and store to Parquet
    data_dir = "out"
    load_data_to_parquet(data_dir)
