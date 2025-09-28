import os


def main():
    data_dir = os.getenv("DATA_DIR", "../data")
    total_size = 0
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.startswith("202") and file.endswith(".csv"):
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                total_size += file_size
                print(f"{file}: {file_size / (1024 * 1024):.2f} MB")

    print(f"Total size of files in {data_dir}: {total_size / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    main()
