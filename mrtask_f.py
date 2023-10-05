import pandas as pd
from mrjob.job import MRJob

# Define a class for analyzing average trip revenue
class MRAvgTripRev(MRJob):

    # Mapper function to process each line of input data
    def mapper(self, _, line):
        rec = line.split(",")  # Split the CSV line into fields
        
        if rec[0] != 'VendorID':  # Exclude the header row
            pickup_date = pd.to_datetime(rec[1])  # Convert pickup date to a datetime object
            trip_revenue = float(rec[16])  # Extract trip revenue (total amount)
            
            # Determine the pickup month, pickup hour (day/night), and weekday/weekend flag
            pickup_month = pickup_date.strftime('%B')  # Name of the month
            if pickup_date.hour in [23, 0, 1, 2, 3, 4, 5]:
                pickup_hour = "night"
            else:
                pickup_hour = "day"
            if pickup_date.day_name() in ['Sunday', 'Saturday']:
                pickup_weekday_weekend_flag = "weekend"
            else:
                pickup_weekday_weekend_flag = "weekday"

            # Emit key-value pairs for analysis
            yield (pickup_month, trip_revenue)  # Key: pickup month, Value: trip revenue
            yield (pickup_hour, trip_revenue)  # Key: pickup hour (day/night), Value: trip revenue
            yield (pickup_weekday_weekend_flag, trip_revenue)  # Key: weekday/weekend flag, Value: trip revenue

    # Reducer function to calculate the mean revenue value for each key
    def reducer(self, key, values):
        total_revenue = sum(values)  # Calculate the total revenue for the key
        total_count = sum(1 for _ in values)  # Count the number of values (trips)
        
        # Calculate the average trip revenue for the key
        average_trip_revenue = total_revenue / total_count if total_count > 0 else 0
        
        yield key, average_trip_revenue  # Emit key-value pairs (key, average trip revenue)

# Main method to run the MapReduce job
if __name__ == '__main__':
    MRAvgTripRev.run()
