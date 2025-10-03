from mrjob.job import MRJob, MRStep
from datetime import datetime

gender_map = {"0": "unknown", "1": "male", "2": "female"}


class MRJobNYCCitiBike(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.extract_csv_fields, reducer=self.reducer),
        ]

    def extract_csv_fields(self, _, line):
        # Skip the header line
        if line.replace('"', "").startswith("ride_id"):
            return

        fields = line.split(",")
        if len(fields) < 13:
            return  # Skip lines that don't have enough fields

        try:
            ride_id = fields[0]
            rideable_type = fields[1]
            started_at = fields[2]
            ended_at = fields[3]
            start_station_name = fields[4]
            start_station_id = fields[5]
            end_station_name = fields[6]
            end_station_id = fields[7]
            start_lat = fields[8]
            start_lng = fields[9]
            end_lat = fields[10]
            end_lng = fields[11]
            member_casual = fields[12]

            started_at = datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S.%f")
            ended_at = datetime.strptime(ended_at, "%Y-%m-%d %H:%M:%S.%f")
            start_year = started_at.year
            start_month = started_at.month
            start_day = started_at.day
            start_hour = started_at.hour

            end_year = ended_at.year
            end_month = ended_at.month
            end_day = ended_at.day
            end_hour = ended_at.hour
            started_at = datetime.timestamp(started_at)
            ended_at = datetime.timestamp(ended_at)
            trip_duration_seconds = int(ended_at - started_at)
            haversine_distance = None  # Placeholder for distance calculation if needed
           
            # yield None, {
            #     "ride_id": ride_id,
            #     "rideable_type": rideable_type,
            #     "started_at": started_at,
            #     "ended_at": ended_at,
            #     "start_station_name": start_station_name,
            #     "start_station_id": start_station_id,
            #     "end_station_name": end_station_name,
            #     "end_station_id": end_station_id,
            #     "start_lat": start_lat,
            #     "start_lng": start_lng,
            #     "end_lat": end_lat,
            #     "end_lng": end_lng,
            #     "member_casual": member_casual,
            #     "trip_duration_seconds": trip_duration_seconds,
            #     "haversine_distance": haversine_distance,
            # }

            # For example, yield start_station_id as key and trip_duration_seconds as value
            yield (start_year, start_month, start_day, start_hour, start_station_id), {
                "type": rideable_type,
                "direction": "out",
                "trip_duration_seconds": trip_duration_seconds,
            }

            yield  (end_year, end_month, end_day, end_hour, end_station_id), {
                "type": rideable_type,
                "direction" : "in",
                "trip_duration_seconds": trip_duration_seconds
            }

        except ValueError:
            print(f"Skipping line due to ValueError: {line}")
            return  # Skip lines with invalid integer values

    def reducer(self, key, values):
        total_trips = 0
        total_duration = 0
        bikes_in = {} 
        bikes_out = {} 

        for value in values:
            if value["direction"] == "in":
                bikes_in[value["type"]] = bikes_in.get(value["type"],0) + 1
            elif value["direction"] == "out":
                bikes_out[value["type"]] = bikes_in.get(value["type"],0) + 1    

            trip_duration_seconds = value["trip_duration_seconds"]
            total_trips += 1
            total_duration += trip_duration_seconds
        if total_trips > 0:
            average_duration = total_duration / total_trips
            yield key, (
                total_trips,
                average_duration,
                bikes_in,
                bikes_out
            )
        else:
            yield key, (0, 0)


def schema_extractor():
    import os
    import glob

    schema_list = []
    seen_schemas = set()
    data_dir = os.getenv("DATA_DIR", "../data")
    for csv_file in glob.glob(os.path.join(data_dir, "*.csv")):
        with open(csv_file, "r") as f:
            header = f.readline().strip()
            header = header.replace('"', "")
            header = header.replace(" ", "")
            header = header.lower()
            if header not in seen_schemas:
                seen_schemas.add(header)
                schema_list.append(header)

            print(f"{csv_file}:\tversion {schema_list.index(header)}")
    for schema in seen_schemas:
        print(schema)


if __name__ == "__main__":
    # schema_extractor()  # Uncomment to run schema extractor instead
    MRJobNYCCitiBike.run()
