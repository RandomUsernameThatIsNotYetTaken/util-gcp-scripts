# Only import libraries
from google.cloud import datastore
import sys
import json
import os
####################################################
####################################################
####################################################
####################################################
#This will upload a sample json file into datastore.
#The json file should have the following format:
# {
#     "key": {
#         "partitionKey": {
#             "projectId": "projectID",
#             "namespace": "namespace"
#         },
#         "path": {
#             "kind": "kind"
#         }
#     },
#     "entities": [
#         {
#             "field1": "Lorem",
#             "field2": "Ipsum",
#             "field3": "123",
#             "field4": "000",
#             "field5": "Data"
#         },
#         {
#             "field1": "Ipsum",
#             "field3": "Lorem",
#             "field4": "000",
#             "field5": "123"
#         }
#     ]
# }

def progressBar(count_value, total, suffix=''):
    bar_length = 100
    filled_up_Length = int(round(bar_length* count_value / float(total)))
    percentage = round(100.0 * count_value/float(total),1)
    bar = '=' * filled_up_Length + '-' * (bar_length - filled_up_Length)
    sys.stdout.write('[%s] %s%s ...%s\r' %(bar, percentage, '%', suffix))
    sys.stdout.flush()

def set_values_entity(fields, data_entry, entity):
  for field in fields:
    if field != 'key':
      if field in data_entry:
        entity[field] = data_entry[field]
      else:
        entity[field] = None
  return entity

def convert(filename, path):
    BASE_DIR = os.path.dirname(os.path.abspath(filename))
    readFile = os.path.join(BASE_DIR, filename)
    #load the file as json
    with open(readFile) as f:
        data = json.load(f)
    #print json data key
    print('\n' + 'Determining kind, projectId, namespace' + json.dumps(data['key']) + '\n')
    #determine kind and namespace
    kind = data['key']['path']['kind']
    namespace = data['key']['partitionKey']['namespace']
    projectId = data['key']['partitionKey']['projectId']

    # Instantiates a client
    datastore_client = datastore.Client(project=projectId, namespace=namespace)
    print ('\n' + 'Datastore instantiated for ' + projectId + ' and namespace ' + namespace + '\n')

    print ('\n' + 'Preparing json data - Please Wait' + '\n')

    #determine attributes of entities by looking at the first item
    fields = list(data['entities'][0].keys())
    #print attributes of entities by looking at the first item
    print('Fields detected: ' + ', '.join(fields))

    #print total number of entities
    print('Total Rows: ' + str(len(data['entities'])))
    totalCount = len(data['entities'])
    i=1

    print('\n' + 'STARTING DATA LOAD' + '\n')
    for data_entry in data['entities']:
        rest_key = datastore_client.key(kind)
        # Prepares the new entity
        entity = datastore.Entity(key=rest_key)
        #set values to entity
        array_entity= set_values_entity(fields,data_entry, entity)
        # Saves the entity
        datastore_client.put(array_entity)
        #based on the amount of entries, generate a progress bar to show the progress
        progressBar(i,totalCount)
        i=i+1
    print('\n' + 'DATA LOAD COMPLETE' + '\n')

#this can be rewritten entirely to use proper file handling and error checking
def determine_input_file():
  # Ask for a folder from command line input
  folder = input("Enter the folder path: ")
  #get the current working directory
  cwd = os.getcwd()
  #if the folder path contains .., then it is a relative path
  if '..' in folder:
    print("Relative path detected, fixing")
    while '..' in folder:
      #go up 1 folder from cwd
      cwd = os.path.dirname(cwd)
      #remove the .. from the folder path
      folder = folder.replace('../', '', 1)
    #append the folder to the current working directory
    folder = os.path.join(cwd, folder)
  print("Folder path: ", folder)
  # Check if the folder exists
  if not os.path.isdir(folder):
    print("Invalid folder path")
    sys.exit(1)
  # Change the current working directory to the specified folder
  os.chdir(folder)
  #list all .json files inside the folder
  files = [f for f in os.listdir(folder) if f.endswith('.json')]
  #if files is empty, exit
  if not files:
    print("No .json files found in the folder")
    sys.exit(1)
  print("Files in the folder: ", files)
  #specify which file to upload
  filename = input("Enter the file name: ")
  return (filename, cwd)


if __name__ == '__main__':
  (filename, path) = determine_input_file()
  os.chdir(path)
  convert(filename, path)
