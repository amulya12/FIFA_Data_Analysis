
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to browse data rows.
# table_id = "your-project.your_dataset.your_table_name"

table_id="amazing-office-347302.test.schema"
# Download all rows from a table.
rows_iter = client.list_rows(table_id)  # Make an API request.

# Iterate over rows to make the API requests to fetch row data.
rows = list(rows_iter)
print("Downloaded {} rows from table {}".format(len(rows), table_id))

# Download at most 10 rows.
rows_iter = client.list_rows(table_id, max_results=10)
rows = list(rows_iter)
print("Downloaded {} rows from table {}".format(len(rows), table_id))

# Specify selected fields to limit the results to certain columns.
table = client.get_table(table_id)  # Make an API request.
fields = table.schema[:2]  # First two columns.
rows_iter = client.list_rows(table_id, selected_fields=fields, max_results=10)
rows = list(rows_iter)
print("Selected {} columns from table {}.".format(len(rows_iter.schema), table_id))
print("Downloaded {} rows from table {}".format(len(rows), table_id))

# Print row data in tabular format.
rows = client.list_rows(table, max_results=10)
format_string = "{!s:<16} " * len(rows.schema)
field_names = [field.name for field in rows.schema]
print(format_string.format(*field_names))  # Prints column headers.
for row in rows:
    print(format_string.format(*row))  # Prints row data.