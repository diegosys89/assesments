from flask import Flask, request, jsonify
from google.cloud import bigquery
import json

app = Flask(__name__)
project_id = 'makikuna-integral'
dataset_id = 'globant_assessment'

with open("config_data.json") as jc:
    data_info = json.load(jc)

@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        #Get the JSON data from the request
        data = request.json

        if not isinstance(data, list):
            return jsonify({"error": "Data must be a list of dictionaries"}), 400

        #Table name from the request
        table_name = request.args.get('table_name')
        print(f"table_name: {table_name}")
        if not table_name:
            return jsonify({"error": "Missing 'table_name' parameter"}), 400
        
        if table_name in [i["table_name"] for i in data_info]:
            client = bigquery.Client(project=project_id)
            table_ref = f"{project_id}.{dataset_id}.{table_name}"
            validated_data = validate_json_list(data, table_name)

            if validated_data == []:
                return jsonify({"error": "No valid data to insert"}), 500

            errors = client.insert_rows_json(table_ref, validated_data)

            if errors:
                return jsonify({"error": "Failed to insert data"}), 500
            else:
                return jsonify({"message": "Data inserted successfully"}), 200
        else:
            return jsonify({"error":"table not found"}, 400)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def validate_json_data(data, bq_schema):
    #Type mapping
    data_types = {"INTEGER":int, "FLOAT":float, "STRING":str}

    for field in bq_schema:
        field_name = field.name
        if field_name not in data:
            print(f"Invalid JSON: {data}. Field name '{field_name}' is not found")
            return False
        expected_type = data_types[field.field_type]
        actual_value = data[field_name]
        if not isinstance(actual_value, expected_type):
            print(f"Invalid JSON: {data}, a value of type '{field.field_type}' was expected for field '{field_name}'")
            return False
    print(f"Valid JSON: {data}")
    return True

def validate_json_list(data_list, table_name):
    schema = [i["schema"] for i in data_info if i["table_name"]==table_name][0]
    bq_schema = [bigquery.SchemaField(i["name"],i["type"]) for i in schema]

    valid_data = []

    for element in data_list:
        is_valid = validate_json_data(element, bq_schema)
        if is_valid:
            valid_data.append(element)

    return(valid_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
