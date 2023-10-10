import json

def read_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return {record['Name']: record for record in data}

def write_json(filename, data):
    with open(filename, 'w') as file:
        json.dump(list(data.values()), file, indent=4)

source_file = "all_files_ns.json"
target_file = "all_files_linode.json"
output_file = "missing_files_linode.json"

source_data = read_json(source_file)
target_data = read_json(target_file)

missing_keys = set(source_data.keys()) - set(target_data.keys())

missing_data = {key: source_data[key] for key in missing_keys}

write_json(output_file, missing_data)
