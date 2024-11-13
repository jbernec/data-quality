from databricks.sdk import WorkspaceClient

# Initialize the client
client = WorkspaceClient()

# List files in a DBFS directory
files = client.dbfs.list('dbfs:/files/')
for file in files:
    print(file.path)
