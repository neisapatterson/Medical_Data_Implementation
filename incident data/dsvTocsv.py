# import pandas as pd
# df = pd.read_csv("sampletest.dsv", sep="}")

import re

def search_and_replace_in_file(input_file, output_file, search_pattern, replacement_text):
    with open(input_file, 'r') as file:
        content = file.read()

    modified_content = re.sub(search_pattern, replacement_text, content)

    with open(output_file, 'w') as file:
        file.write(modified_content)

if __name__ == "__main__":
    # Example usage:
    input_file_path = 'PREF_NAME_CODE_TABLE.dsv'
    output_file_path = 'COPY_PREF_NAME_CODE_TABLE.csv'
    # input_file_path = 'PracticeReplace'
    # output_file_path = 'PracticeReplace3.csv'

    #First delete "  
    search_pattern = r"\""  # First \" Second , Replace 'world' with another word
    replacement_text = '' # First '' Second
    search_and_replace_in_file(input_file_path, output_file_path, search_pattern, replacement_text)


    input_file_path = 'COPY_PREF_NAME_CODE_TABLE.csv'

    #Second delete ,
    search_pattern = r","  # First \" Second , Replace 'world' with another word
    replacement_text = '' # First '' Second
    search_and_replace_in_file(input_file_path, output_file_path, search_pattern, replacement_text)

    #Third replace | with ,
    search_pattern = r"\|"  # First \" Second , Replace 'world' with another word
    replacement_text = ',' # First '' Second
    search_and_replace_in_file(input_file_path, output_file_path, search_pattern, replacement_text)

    print(f"Search and replace completed. Check the '{output_file_path}' file.")