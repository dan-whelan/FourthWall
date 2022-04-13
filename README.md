# FourthWall

## Description
FourthWall is a database monitoring service that allows for easy access to metadata 
relating to PostGresDBs. Using polling these are stored in an external file system 
that can be accessed through the use of an authenticated API. 

## Setup


## API Calls
To create an API request to the service the following methods can be used:
- Terminal
    While the project is running input following for unix terminal for general GET request
    
    curl -X GET https://localhost:<port_number>/api/<api_call>/<param_1>/... -H "ApiKey: <api_key>"

- Calls
    For Statistics:
        
        curl -X GET https://localhost:<port_number>/api/statistics/{schema}/{table} -H "ApiKey: <api_key>"
   
    For Long Running Queries:
        
        curl -X GET https://localhost:<port_number>/api/queries/{from}/{to} -H "ApiKey: <api_key>"
   
    For Index Usage 
        
        (Specified Index):
        curl -X GET https://localhost:<port_number>/api/indexes/{schema}/{table}/{indexName} -H "ApiKey: <api_key>"
        
        (All Indexes):
        curl -X GET https://localhost:<port_number>/api/indexes/{schema}/{table} -H "ApiKey: <api_key>"
    
    For Explain Analyse POST request:
        
        curl -X POST https://localhost:<port_number>/api/query/ -H "ApiKey: <api_key>" -H "Content-Type: application/json" -d '{"query": "<query>;"}'
    
    For All Table Data:
        
        curl -X GET  https://localhost:<port_number>/api/data/{schema}/{table} -H "ApiKey: <api_key>"

## Authentication
A basic authentication using an API key is used for this template, in later iterations a more secure authentication system would be used. Source stored in Attributes.

## .env File Specification

### Necessary Environment Variables
- DATABASE=nameOfDatabase 
- PASSWORD=password
- USERID=userid
- SERVER=server
- PORT=portnumber
- BASEPATH=base/path/to/store/statistics

### Optional Environment Variables
- POLLING_INTERVAL_MILLISECONDS=15000
- LONG_RUNNING_QUERIES_DEFAULT_TIME_SECS=5


