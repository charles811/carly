# input_file_path = '/opt/airflow/directory/sideprojects/carly/input/sbscription_eventsV1.txt'
# output_file_path = '/opt/airflow/directory/sideprojects/carly/input/sbscription_eventsV2.txt'

# with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
#     for line in input_file:
#         line = line.strip()
#         if line.endswith('}'):
#             line += ','
#         output_file.write(line + '\n')




import json

input_file_path = '/opt/airflow/directory/sideprojects/carly/input/sbscription_eventsV1.txt'
output_json_file_path = '/opt/airflow/directory/sideprojects/carly/input/sbscription_events.json'

# Read each line from the text file, parse as JSON, and store in a list
json_data = []
with open(input_file_path, 'r') as input_file:
    for line in input_file:
        json_data.append(json.loads(line))

# Write the list of parsed JSON objects to a new JSON file
with open(output_json_file_path, 'w') as output_json_file:
    json.dump(json_data, output_json_file, indent=2)



