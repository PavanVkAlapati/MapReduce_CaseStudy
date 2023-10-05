from mrjob.job import MRJob
from mrjob.step import MRStep

class MRVendorCount(MRJob):
    
    # Define the multi-step process for the MapReduce job
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

    # Mapper function to process each line of input data
    def mapper(self, _, line):
        rec = line.split(",")  # Split the CSV line into fields
        
        VendorID = rec[0]
        TotAmt = rec[16]
        
        # Exclude the header row
        if TotAmt != 'total_amount':
            if VendorID == "1":
                Vendor = 'Creative Mobile Technologies'
            else:
                Vendor = 'VeriFone Inc.'
            yield (Vendor, 1)  # Emit (Vendor, 1) for each trip

    # Reducer function to sum up the counts for each vendor
    def reducer(self, key, values):
        yield None, (sum(values), key)  # Emit (None, (total_count, Vendor)) for each vendor

    # Reducer function to find the vendor with the most trips
    def reducer_find_max(self, _, vendor_pairs):
        yield max(vendor_pairs)  # Emit the vendor with the highest trip count

if __name__ == '__main__':
    MRVendorCount.run()
