import json

def json_to_sql(json_data, table_name):
    sql_commands = []

    for entry in json_data:
        columns = ', '.join(entry.keys())
        values = ', '.join(["'{}'".format(str(value).replace("'", "\\'")) for value in entry.values()])
        sql_command = "INSERT INTO {} ({}) VALUES ({});".format(table_name, columns, values)
        sql_commands.append(sql_command)

    return '\n'.join(sql_commands)

if __name__ == '__main__':
    # Load your JSON data
    with open('data_outpu/frost_tool_dict.json', 'r') as f:
        data = json.load(f)

    sql_content = json_to_sql(data, 'zip_frost_lookup')

    # Save the SQL commands to a file
    with open('frost_tool.sql', 'w') as f:
        f.write(sql_content)
