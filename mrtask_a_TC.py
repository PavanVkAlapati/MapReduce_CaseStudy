from mrjob.job import MRJob
from mrjob.step import MRStep

class MRVendorCount(MRJob):

    # Define command-line arguments, including the vendor mapping file
    def configure_args(self):
        super(MRVendorCount, self).configure_args()
        self.add_file_arg('--vendor_mapping', help='Path to vendor mapping file')

    # Mapper function to process each line of input data
    def mapper(self, _, line):
        fields = line.split(",")  # Split the CSV line into fields
        if fields[16] != 'total_amount':  # Check if it's not the header row
            vendor_id = int(fields[0])  # Extract vendor ID
            total_amount = float(fields[16])  # Extract total amount

            vendor_mapping = self.get_vendor_mapping()  # Get vendor mapping from file
            vendor_name = vendor_mapping.get(vendor_id, 'Unknown Vendor')  # Map vendor ID to vendor name

            yield vendor_name, total_amount  # Emit (vendor_name, total_amount) key-value pair

    # Reducer function to sum total amounts for each vendor
    def reducer(self, vendor, total_amounts):
        total_revenue = sum(total_amounts)  # Calculate total revenue for the vendor
        yield vendor, total_revenue  # Emit (vendor, total_revenue) result

    # Helper method to load vendor mapping from the provided file
    def get_vendor_mapping(self):
        vendor_mapping = {}
        with open(self.options.vendor_mapping, 'r') as file:
            for line in file:
                vendor_id, vendor_name = line.strip().split(',')
                vendor_mapping[int(vendor_id)] = vendor_name
        return vendor_mapping

    # Define the map-reduce steps
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    MRVendorCount.run()
