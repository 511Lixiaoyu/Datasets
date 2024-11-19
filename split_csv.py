import csv
import os
 
def split_csv(input_file, output_dir, chunk_size):
    with open(input_file, 'r', newline='') as file:
        reader = csv.reader(file)
        chunk = []
        count = 0
        
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                filename = f"{output_dir}/chunk_{count}.csv"
                with open(filename, 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerows(chunk)
                count += 1
                chunk = []
        
        # Write the last chunk
        if chunk:
            filename = f"{output_dir}/chunk_{count}.csv"
            with open(filename, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerows(chunk)
 
input_file = '..\\datasets\\utd19_u.csv'
output_dir = '..\\datasets\\split_csv_100000'
chunk_size = 1000000  # row number
 
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
 
split_csv(input_file, output_dir, chunk_size)
