## Use this for Azure AD authentication
from msrestazure.azure_active_directory import AADTokenCredentials

## Required for Data Lake Storage Gen1 account management
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.mgmt.datalake.store.models import DataLakeStoreAccount

## Required for Data Lake Storage Gen1 filesystem management
from azure.datalake.store import core, lib, multithread

 # Common Azure imports
import adal
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup

## Use these as needed for your application
import logging, getpass, pprint, uuid, time

import pandas as pd


## datalake parameters ##
authority_host_uri = 'https://login.microsoftonline.com'
RESOURCE = 'https://management.core.windows.net/'
#default_datalake = 'cernerdatalake'


################################################################
# PARSE AUTH KEY VALUES
################################################################
################################################################

def getBeaconKeys(keys):
    if bool(keys):
        # read config file
        #key_data = literal_eval(keys)
        client_id       = keys['client_id']
        client_secret   = keys['client_secret']
        tenant_id       = keys['tenant_id']
        return(client_id,client_secret,tenant_id)
    else: 
        raise Exception('Error: API keys not provided')

    return(client_id,client_secret,tenant_id)


################################################################
# OBTAIN BEACON TOKEN
################################################################

def connectBeacon(keys):
    """Establishes a connection to Beacon
    Args:
        null
    Returns:
        token : an authrization token for Beacon access 
    """

    client_id,client_secret,tenant_id = getBeaconKeys(keys)      

    authority_uri = authority_host_uri + '/' + tenant_id

    ## establish authentication authority context
    context = adal.AuthenticationContext(authority_uri, api_version=None)

    ## obtiain a datalake management token
    ##mgmt_token = context.acquire_token_with_client_credentials(RESOURCE, client_id, client_secret)

    ## obtain resource mgmt credentials
    ##armCreds = AADTokenCredentials(mgmt_token, client_id, resource=RESOURCE)

    ## obtain session authorization token
    token = lib.auth(tenant_id = tenant_id,
                client_secret = client_secret,
                client_id = client_id,
                resource = RESOURCE)

    return (token)


################################################################
# ESTABLISH BEACON CLIENT SESSION
################################################################

def getClient(token):
    """Creates an instance of a datalake client
    Args:
        token (obj): connection token
        store_name (str): the datalake to connect to as a string
    Returns:
        client session: a datalake client
    """

    adlsFileSystemClient = core.AzureDLFileSystem(token,store_name='cernerdatalake')

    return adlsFileSystemClient


################################################################
# TOOLKIT FUNCTIONS
################################################################
################################################################


################################################################
# getFile() aquires and converts a target file to a pandas dataframe
# provided the client instance, file path and file format.
################################################################


class MyADL(object):

    def __init__(self,token):
        self._adlsFileSystemClient = core.AzureDLFileSystem(token,store_name='cernerdatalake')

    def getFile(self,file_path,file_format='tsv'):
        """Returns a pandas dataframe from the azure datalake using the provided file path
        Args:
        adl_client (obj): An Azure Datalake client instance
        file_path ([type]): The full path to the file in the datalake
        file_format ([type]): The file format of the file in the datalake.  default=tsv
        Returns:
        client session: a pandas dataframe of the file contents
        """
        if file_format == 'tsv':
            with self._adlsFileSystemClient.open(file_path) as f:
                df = pd.read_csv(f,sep='\t',low_memory=False)

        elif file_format == 'csv':
            with self._adlsFileSystemClient.open(file_path) as f:
                df = pd.read_csv(f,sep=',')

        return df

    def __exit__(self):
        self._adlsFileSystemClient.close()
        

################################################################    


