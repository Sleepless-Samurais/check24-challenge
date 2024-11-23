import json


def extract_attributes(data, attributes):
    """
    Recursively search for specific attributes in the given JSON data.

    :param data: JSON data (dictionary or list)
    :param attributes: List of attribute names to search for
    :return: Dictionary containing the extracted attributes
    """
    results = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if key in attributes:
                results[key] = value
            elif isinstance(value, (dict, list)):
                nested_results = extract_attributes(value, attributes)
                results.update(nested_results)
    elif isinstance(data, list):
        for item in data:
            nested_results = extract_attributes(item, attributes)
            results.update(nested_results)
    return results


def process_file(input_file, output_file):
    """
    Process the input file to extract attributes and format the output.

    :param input_file: Path to the input file containing one-line JSONs
    :param output_file: Path to the output file
    """
    attributes_to_extract = {"search_config", "write_config"}
    extracted_data = []

    with open(input_file, "r") as infile:
        for line in infile:
            try:
                json_data = json.loads(line.strip())
                extracted = extract_attributes(json_data, attributes_to_extract)
                if extracted:
                    # Add required structure
                    output_line = {
                        "requestType": json_data.get("requestType", "UNKNOWN"),
                        "timestamp": json_data.get("timestamp", "UNKNOWN"),
                    }
                    output_line.update(extracted)
                    extracted_data.append(output_line)
            except json.JSONDecodeError as _:
                print(f"Skipping invalid JSON line: {line.strip()}")

    with open(output_file, "w") as outfile:
        for item in extracted_data:
            json.dump(item, outfile)
            outfile.write("\n")


# Example usage
input_file_path = (
    "019358aa-b2ae-74bb-bc78-51dfd87f4169-1.log"  # Replace with your input file path
)
output_file_path = "output.jsonl"  # Replace with your desired output file path
process_file(input_file_path, output_file_path)
