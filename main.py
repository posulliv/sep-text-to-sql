from cli_helpers import tabular_output
from prompt_toolkit import prompt
from prompt_toolkit import print_formatted_text, HTML
from termcolor import colored
import configparser
import trino
import openai

def trino_connection(trino_config):
    return trino.dbapi.connect(
        host = trino_config['host'],
        port = trino_config['port'],
        user = trino_config['user'],
        catalog = trino_config['catalog'],
        schema = trino_config['schema'],
        http_scheme = trino_config['http_scheme'],
        auth = trino.auth.BasicAuthentication(trino_config['user'], trino_config['password']),
        verify = trino_config['verify_certs'].lower() == 'true'
    )

def trino_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    headers = [column[0] for column in cursor.description]
    print("\n".join(tabular_output.format_output(iter(data), headers, format_name='simple')))

def schema_metadata(conn, catalog, schema):
    cursor = conn.cursor()
    cursor.execute('show tables')
    data = cursor.fetchall()
    tables = [row[0] for row in data]
    table_metadata = []
    for table in tables:
        cursor.execute('show create table %s' % table)
        data = cursor.fetchall()
        table_metadata.append(data[0][0])
    return table_metadata

def generate_chatgpt_messages(metadata, user_input):
    purpose = "You are a Trino SQL expert that can translate english language queries to Trino compatible SQL. You respond with the SQL query only with no semicolon and no explanations."
    sql_prompt = 'Generate Trino compatible SQL query for the following english query: %s\nColumn aliases should be quoted. You should not use DATE_FORMAT to extract from dates.' % user_input
    schema_prompt = '\nSchema of all relevant tables:%s\n' % '\n'.join(metadata)
    return [
        {"role": "system", "content": purpose},
        {"role": "user", "content": sql_prompt + schema_prompt}
    ]

def generate_trino_query(messages, model="gpt-3.5-turbo"):
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0
    )
    return response.choices[0].message["content"].rstrip(";")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    trino_config = config['trino']
    trino_conn = trino_connection(trino_config)
    print('gathering metadata for tables in %s.%s' % (trino_config['catalog'], trino_config['schema']))
    tables_metadata = schema_metadata(trino_conn, trino_config['catalog'], trino_config['schema'])
    openai_config = config['openai']
    openai.api_key = openai_config['apikey']
    while True:
        input_text = prompt('what do you want to know?\n')
        if input_text == 'quit':
            break
        if input_text == 'refresh_metadata':
            tables_metadata = schema_metadata(trino_conn, trino_config['catalog'], trino_config['schema'])
        chatgpt_messages = generate_chatgpt_messages(tables_metadata, input_text) 
        generated_trino_query = generate_trino_query(chatgpt_messages)
        print(colored("Running Starburst query:\n%s" % generated_trino_query, 'green'))
        trino_query(trino_conn, generated_trino_query)
