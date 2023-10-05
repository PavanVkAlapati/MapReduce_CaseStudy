from mrjob.job import MRJob
from mrjob.step import MRStep

# Define a MRJob class for payment type and count
class MRPmtTypeCount(MRJob):

    # Define steps for the MapReduce job, including mapper, reducer, and sorting
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,  # Specify the mapper function
                reducer=self.reducer,  # Specify the reducer function
            ),
            MRStep(
                reducer=self.sorting_reducer_data  # Specify the sorting reducer function
            )
        ]

    # Mapper function to process each line of input data
    def mapper(self, _, line):
        rec = line.split(",")  # Split the CSV line into fields
        payment_type = rec[9]  # Payment type
        
        # Ignore the header row
        if payment_type != 'payment_type':
            yield (payment_type, 1)  # Emit (payment_type, 1) for each payment type

    # Reducer function to aggregate and count payment types
    def reducer(self, key, values):
        yield None, (sum(values), key)  # Emit (None, (count of payment type, payment type))

    # Sorting reducer function to sort the data
    def sorting_reducer_data(self, _, payment_type_count_pairs):
        for count, payment_type in sorted(payment_type_count_pairs):
            yield (payment_type, count)  # Emit sorted payment type and count pairs

# Main method to run the MapReduce job
if __name__ == '__main__':
    MRPmtTypeCount.run()
