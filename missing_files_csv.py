import csv

def read_csv(filename):
    data = {}
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = (row['Name'])
            data[key] = row
    return data

def write_csv(filename, data):
    headers = ['Path', 'Name', 'Size', 'ModTime', 'IsDir']
    with open(filename, 'w') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for item in data.values():
            writer.writerow(item)


source_file = "flattened_output.csv"
target_file = "linode_flattened_output.csv"
output_file = "missing-in-linode.csv"

source_data = read_csv(source_file)
target_data = read_csv(target_file)

missing_keys = set(source_data.keys()) - set(target_data.keys())

missing_data = {key: source_data[key] for key in missing_keys}

write_csv(output_file, missing_data)
