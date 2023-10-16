import json

# Load the JSON data
with open('your_json_data.json', 'r') as json_file:
    data = json.load(json_file)

# Create a dictionary to store groups
groups = {}

# Iterate through the data
for item in data:
    path = item['Path']
    # Extract the group key by finding the last "/"
    group_key = path.rsplit('/', 1)[0] + '/'

    # Initialize the group if it doesn't exist
    if group_key not in groups:
        groups[group_key] = {'mp4': [], 'ism': None, 'json_line': []}

    # Store the entire JSON line in the group
    groups[group_key]['json_line'].append(item)

    # Check if the file is an mp4 or ism and store it accordingly
    if path.endswith('.mp4'):
        groups[group_key]['mp4'].append(path)
    elif path.endswith('.ism'):
        groups[group_key]['ism'] = path

# Create a list to store missing ism mp4 pairs
missing_pairs = []

# Check for missing ism files
for group_key, group_data in groups.items():
    if group_data['ism'] is None:
        # If there is no ism file in the group, consider all JSON lines as missing
        missing_pairs.extend(group_data['json_line'])

# Write missing pairs to another JSON file without formatting
with open('missing_pairs.json', 'w') as missing_file:
    for i, item in enumerate(missing_pairs):
        missing_file.write(json.dumps(item))
        if i < len(missing_pairs) - 1:
            missing_file.write(',')

# Optionally, you can print the missing pairs for reference
print("Missing ism files:")
for item in missing_pairs:
    print(json.dumps(item))
