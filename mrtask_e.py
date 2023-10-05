import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep

# Define a class for the MapReduce job
class MRAvgTipRevRatio(MRJob):

    # Define the steps for the MapReduce job
    def steps(self):
        return [
            MRStep(
                mapper=self.MainMapper,  # Specify the mapper function
                reducer=self.MainReducer,  # Specify the reducer function
            ),
            MRStep(
                reducer=self.sorting_reducer_data  # Specify the sorting reducer function
            )
        ]

    # Mapper function for the main job
    def MainMapper(self, _, line):
        rec = line.split(",")  # Split the CSV line into fields
        pickup_location = rec[7]  # Pickup location
        
        # Exclude the header row
        if rec[0] != 'VendorID':
            tip = float(rec[13])  # Tip amount
            revenue = float(rec[16])  # Trip revenue (total amount)

            # Check for divide by zero issue, avoid division by zero
            if revenue > 0:
                tip_rev_ratio = tip / revenue  # Calculate tip-to-revenue ratio
            else:
                tip_rev_ratio = tip
            
            yield (pickup_location, tip_rev_ratio)  # Emit (pickup location, tip-to-revenue ratio)

    # Reducer function for the main job
    def MainReducer(self, key, values):
        average_tip_rev_ratio = sum(values) / len(key)  # Calculate the average tip-to-revenue ratio
        yield None, (average_tip_rev_ratio, key)  # Emit (None, (average tip-to-revenue ratio, pickup location))

    # Sorting reducer function
    def sorting_reducer_data(self, _, trip_rev_ratios):
        for tr_ratio, key in sorted(trip_rev_ratios):
            yield (tr_ratio, key)  # Emit sorted key-value pairs

# Main method to run the MapReduce job
if __name__ == '__main__':
    MRAvgTipRevRatio.run()
