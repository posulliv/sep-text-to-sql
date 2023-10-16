# SEP Text to SQL CLI

Simple python based CLI for natural language to SQL using ChatGPT.

To configure the tool edit the `config.ini` file and add information
for your SEP cluster and your OpenAI API key.

Currently, the tool just gets metadata for the schema specified in
your `config.ini` file. Thus all the tables you wish to generate SQL
for should be in this schema.
