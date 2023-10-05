import pandas as pd
from mrjob.job import MRJob

# Define a class for the MapReduce job to calculate average trip time
class MRAvgTripTime(MRJob):

    # Mapper function to process each line of input data
    def mapper(self, _, line):
        rec = line.split(",")  # Split the CSV line into fields
        pickup_location = rec[7]  # Pickup location code
        
        # Check for and eliminate the header row
        if rec[0] != 'VendorID':
            pickup_time = pd.to_datetime(rec[1])  # Convert pickup time to a datetime object
            drop_time = pd.to_datetime(rec[2])  # Convert drop time to a datetime object
            trip_time = (drop_time - pickup_time).total_seconds()  # Calculate trip time in seconds
            
            yield (pickup_location, trip_time)  # Emit (pickup location, trip time in seconds)

    # Reducer function to calculate the average trip time
    def reducer(self, key, values):
        total_trip_time = sum(values)  # Calculate the total trip time in seconds
        total_count = sum(1 for _ in values)  # Count the number of values (trips)
        
        # Calculate the average trip time in seconds
        average_trip_time = total_trip_time / total_count if total_count > 0 else 0
        
        yield key, average_trip_time  # Emit (pickup location, average trip time in seconds)

# Main method to run the MapReduce job
if __name__ == '__main__':
    MRAvgTripTime.run()