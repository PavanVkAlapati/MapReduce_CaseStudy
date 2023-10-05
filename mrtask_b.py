from mrjob.job import MRJob
from mrjob.step import MRStep

class MRLocRev(MRJob):

    # Define MR job steps for mapper, reducer, and finding the maximum
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            ),
            MRStep(
                reducer=self.reducer_find_max
            )
        ]

    # Mapper to map key-value pairs
    def mapper(self, _, line):
        rec = line.split(",")  # Splitting the input line
        pickup_location = rec[7]  # Pickup location
        total_amount = rec[16]  # Total amount (revenue)

        # Skip the header row
        if total_amount != 'total_amount':
            yield pickup_location, float(total_amount.strip(','))

    # Reducer aggregates revenue per pickup location
    def reducer(self, key, values):
        yield None, (sum(values), key)

    # Finding the maximum revenue and associated pickup location
    def reducer_find_max(self, _, revenue_location_pairs):
        yield max(revenue_location_pairs)

if __name__ == '__main__':
    MRLocRev.run()
