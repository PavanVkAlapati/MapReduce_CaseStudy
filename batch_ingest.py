import csv 
import happybase  
import glob  
# Define the HBase table and column family  
table_name = 'trip_data_batch'  
column_family = 'cf'  

# connecting to HBase server and opening the table
connection = happybase.Connection(host='ec2-3-80-189-243.compute-1.amazonaws.com')
table = connection.table(table_name)
csv_dir_path = '/home/hadoop/files/file'
csv_files = glob.glob(csv_dir_path + '/*.csv')

# open CSV file and read the data
for file in csv_files:
	with open(file, 'r') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
# defining row key and column values for each row
			row_key = row['ID']
			column_values = {
				f'{column_family}:VendorID': row['VendorID'],
				f'{column_family}:tpep_pickup_datetime': row['tpep_pickup_datetime'],
				f'{column_family}:tpep_dropoff_datetime': row['tpep_dropoff_datetime'],
				f'{column_family}:passenger_count': row['passenger_count'],
				f'{column_family}:trip_distance': row['trip_distance'],
				f'{column_family}:RatecodeID': row['RatecodeID'],
				f'{column_family}:store_and_fwd_flag': row['store_and_fwd_flag'],
				f'{column_family}:PULocationID': row['PULocationID'],
				f'{column_family}:DOLocationID': row['DOLocationID'],
				f'{column_family}:payment_type': row['payment_type'],
				f'{column_family}:fare_amount': row['fare_amount'],
				f'{column_family}:extra': row['extra'],
				f'{column_family}:mta_tax': row['mta_tax'],
				f'{column_family}:tip_amount': row['tip_amount'],
				f'{column_family}:tolls_amount': row['tolls_amount'],
				f'{column_family}:improvement_surcharge': row['improvement_surcharge'],
				f'{column_family}:total_amount': row['total_amount'],
				f'{column_family}:congestion_surcharge': row['congestion_surcharge'],
				f'{column_family}:airport_fee': row['airport_fee']
				}
# inserting the row into the HBase table
			table.put(row_key.encode('utf-8'), column_values)
# Close the connection
connection.close()
