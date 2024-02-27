# Only import libraries
from google.cloud import datastore
import sys

def delete(projectId, namespace, kind):
    # Instantiates a client
    datastore_client = datastore.Client(project=projectId, namespace=namespace)
    print ('\n' + 'Datastore instantiated for ' + projectId + ' and namespace ' + namespace + '\n')
    print ('\n' + 'Preparing to delete all keys for kind ' + kind + '\n')
    query = datastore_client.query(kind=kind)
    query.keys_only()
    keys = list(query.fetch())
    print('\n' + 'Deleting ' + str(len(keys)) + ' keys' + '\n')
    datastore_client.delete_multi(keys)
    print('\n' + 'All keys for kind ' + kind + ' have been deleted' + '\n')

#this can be rewritten entirely to use proper file handling and error checking
def determine_inputs():
  print('\n' + 'THIS WILL DELETE ALL KEYS FOR THE GIVEN NAMESPACE AND KIND.' + '\n')
  print('\n' + 'THIS WILL DELETE ALL KEYS FOR THE GIVEN NAMESPACE AND KIND.' + '\n')

  continue_ = input("Do you wish to continue: (Y/N) ")
  if continue_ != 'Y':
    sys.exit(1)
  projectId = input("Enter the projectId: ")
  namespace = input("Enter the namespace: ")
  kind = input("Enter the kind: ")

  print('\n' + 'THIS WILL DELETE ALL KEYS FOR THE GIVEN NAMESPACE AND KIND.' + '\n')
  print('\n' + 'PROJECT ID: ' + projectId + '\n')
  print('\n' + 'NAMESPACE: ' + namespace + '\n')
  print('\n' + 'kind: ' + kind + '\n')

  continue_ = input("Do you wish to continue: (Y/N) ")
  if continue_ != 'Y':
    sys.exit(1)

  return (projectId, namespace, kind)

if __name__ == '__main__':
  (projectId, namespace, kind) = determine_inputs()
  delete(projectId, namespace, kind)
