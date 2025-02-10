# azure-cosmos-distributed-bulk-sample
## Distributed bulk sample in Azure Cosmos DB
### Summary
This sample shows how a workload can ingest a large amount of data - billions of documents into Cosmos DB. The sample allows distributing the ingestion of a set of files containing the input (this sample uses text files where each line contains the json with the document's payload). To allow making the ingestion restartable and tracking of the status of the ingestion pipeline a metadata container in Cosmos DB is used. Each ingestion job will track status for each input file - and each file is split into a number of smaller batches. The ingestion will then process batch by batch and update the status. If the application stops and needs to be restarted the smallest amount of work that would need to be repeated is a batch.
The input files are expected to be stored in an Azure Storage container.

### Configuration
The sample uses various configuration options to determine authentication, which Storage account to expect the input files in and which Cosmos DB resource to use for data ingestion etc. These configuration options need to be set via Java system properties environment variables - if both are specified the system property will be used. The ´Importance´ column expresses how important it is to set the configuration for this setting correctly - ´LOW´means the setting is optional, and it is not expected that the default needs to be changed often. `HIGH` means this setting needs to be carefully evaluated depending on the workload - `REQUIRED` means the setting has to be specified.

| System property name                             | Environment variable name                        | Importance | Default value                                                                                                                                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------------------------------------------------|--------------------------------------------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| COSMOS.ACCOUNT_ENDPOINT                          | COSMOS_ACCOUNT_ENDPOINT                          | REQUIRED   | n/a                                                                                                                                                                   | The service endpoint - like https://<YOUR_ACCOUNT>.documents.azure.com                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| COSMOS.CDB_DATABASE_NAME                         | COSMOS_CDB_DATABASE_NAME                         | REQUIRED   | n/a                                                                                                                                                                   | The name of the Cosmos DB database in which the data and the Job metadata should be stored.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| COSMOS.CDB_CONTAINER_NAME                        | COSMOS_CDB_CONTAINER_NAME                        | REQUIRED   | n/a                                                                                                                                                                   | The name of the container in which the data should be stored. The sample uses `/id`as partition key - when choosing a different partition key some code changes will be required.                                                                                                                                                                                                                                                                                                                                      |
| COSMOS.LOCAL_BLOB_STORAGE_CACHE_DIR              | COSMOS_LOCAL_BLOB_STORAGE_CACHE_DIR              | REQUIRED   | n/a                                                                                                                                                                   | The path to a directory in the local file system which can be used to cache files downloaded from Azure blob storage.                                                                                                                                                                                                                                                                                                                                                                                                  |
| COSMOS.BLOB_STORAGE_ACCOUNT_NAME                 | COSMOS_BLOB_STORAGE_ACCOUNT_NAME                 | REQUIRED   | n/a                                                                                                                                                                   | The name of the Azure blob storage account in which the input files are stored.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| COSMOS.BLOB_STORAGE_CONTAINER_NAME               | COSMOS_BLOB_STORAGE_CONTAINER_NAME               | REQUIRED   | n/a                                                                                                                                                                   | The name of the Azure blob storage container in which the input files are stored.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| COSMOS.MAX_RECORDS_PER_BATCH                     | COSMOS_MAX_RECORDS_PER_BATCH                     | HIGH       | 5,000                                                                                                                                                                 | The number of documents/lines that should be tracked ina  single batch. For large amount of data to be ingested this value should be relatively high - like in the order of ten's or hundred's of thousand documents when the documents are small. The number of documents per batch determines how much memory on the workers is used to buffer documents - so, depends on VM size of the worker and the size of the documents.                                                                                       |
| COSMOS.MAX_CONCURRENT_BATCHES_PER_MACHINE        | COSMOS_MAX_CONCURRENT_BATCHES_PER_MACHINE        | HIGH       | 8                                                                                                                                                                     | Determines on how many threads batches (chunks of files) are processed - usually this should be around 25%  to 100% of number of CPU cores                                                                                                                                                                                                                                                                                                                                                                             |
| COSMOS.AAD_MANAGED_IDENTITY_ID                   | COSMOS_AAD_MANAGED_IDENTITY_ID                   | HIGH       | The client-id of the managed identity to be used - if not specified picks one based on DefaultAzureCredential logic - if specified, it will always use that identity. |
| COSMOS.AAD_TENANT_ID                             | COSMOS_AAD_TENANT_ID                             | HIGH       | The AAD tenant id for the Azure resources and identities.                                                                                                             |
| COSMOS.CDB_JOB_CONTAINER_NAME                    | COSMOS_CDB_JOB_CONTAINER_NAME                    | HIGH       | Jobs                                                                                                                                                                  | The name of the container in which the Job metadata is stored for tracking teh ingestion status. This container needs to have a partition key `/pk` and it should usually be provisioned with AutoScale of max. throughput between 4,000 and 10,000 RU/s - depending on the number of files/batches ot import.                                                                                                                                                                                                         |
| COSMOS.MAX_RETRY_COUNT                           | COSMOS_MAX_RETRY_COUNT                           | LOW        | 20                                                                                                                                                                    | The number of times transient failures for ingesting one specific document are going to be retried.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| COSMOS.AAD_LOGIN_ENDPOINT                        | COSMOS_AAD_LOGIN_ENDPOINT                        | LOW        | https://login.microsoftonline.com/                                                                                                                                    | Only needs to be modified in non-public Azure clouds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| COSMOS.INITIAL_MICRO_BATCH_SIZE                  | COSMOS_INITIAL_MICRO_BATCH_SIZE                  | LOW        | 1                                                                                                                                                                     | The bulk ingestion feature in Cosmos DB allows streaming changes to the service by physical partition. This initial micro batch size determines the initial target size of a single request sent to a Cosmos DB partition. If the value is too high it could initially result in very high number of throttled requests - over time the sample will tune the micro batch size automatically to maintain a healthy throttling rate (enough to saturate the throughput without wasting too many resources on throttling. |
| COSMOS.MAX_MICRO_BATCH_SIZE                      | COSMOS_MAX_MICRO_BATCH_SIZE                      | LOW        | 100                                                                                                                                                                   | The maximum number of documents sent to the Cosmos DB service for a physical partition in a single request. Has to be between 1 and 100.                                                                                                                                                                                                                                                                                                                                                                               |

### Running the sample

#### Preparation
1) Create the target container
Create the container the data should be ingested in. Ensure the number of physical partitions created is high enough to accommodate the data being ingested. The explanation about how to achieve this is located [here](https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/scenarios/Ingestion.md#creating-a-new-container-if-the-ingestion-via-the-cosmos-spark-connector-is-for-the-initial-migration) - 
The sample expects that the container uses `/id` as partition key - if you want to use a different partition key, some code changes will be required first. When creating the container please ensure the partition key definition is set to `/id` and the indexing policy is updated to exclude as many properties as possible (each to-be-indexed property will add to the RU charge for write operations, so the fewer properties are indexed, the lower the RU charge for each write operation).
The ideal index policy for the target container would look like this:

```xml
{
    "indexingMode": "consistent",
    "automatic": true,
    "includedPaths": [
        
    ],
    "excludedPaths": [
        {
            "path": "/*"
        },
        {
            "path": "/\"_etag\"/?"
        }
    ]
}
```

2) Create the Job container to use for tracking status etc. with metadata documents. This container has to use a partition key definition of `/pk` - and the standard indexing policy should be used.

3) Specify the environment variables according to the (Configuration)[#Configuration] section above.

#### Building the application
You can use `mvn install` to build the jar file. The application will produce a fat-jar (one single jar file containing all dependencies). This file will be located in the target folder and have a name similar to `azure-cosmos-distributed-bulk-sample-1.0-SNAPSHOT-jar-with-dependencies.jar`.

#### Creating a new ingestion job
Each ingestion will be tracked in a `Job` - the job and it's batches (fragments of input files) is tracked in a Cosmos DB Container used to store the metadata. The workers will then load-balance the ingestion across these batches.

To create a new ingestion job - run the following command line
```
java -cp ./azure-cosmos-distributed-bulk-sample-1.0-SNAPSHOT-jar-with-dependencies.jar com.azure.cosmos.samples.distributedbulk.Main create SampleJob01 ^.*\.json$
```

A slightly different syntax can be used to create the job quicker when you know that all your input files have the same number of lines/documents. In this case not each file is downloaded from Storage to count the target number of lines - but this step is done only for one file and then the same number of lines is used for all files.
```
java -cp ./azure-cosmos-distributed-bulk-sample-1.0-SNAPSHOT-jar-with-dependencies.jar com.azure.cosmos.samples.distributedbulk.Main createUnifrom SampleJob01 ^.*\.json$
```

Instead of `SampleJob01`use a unique identifier for your job. The `^.*\.json$` regex is used to identify the search pattern to find the files to be ingested (in the Storage account/container defined via the environment variables)

** NOTE: ** please only execute the command to create a job from a single worker


#### Ingesting the data
This step can be executed from multiple workes. On each worker execute the following command.

```
java -cp ./azure-cosmos-distributed-bulk-sample-1.0-SNAPSHOT-jar-with-dependencies.jar com.azure.cosmos.samples.distributedbulk.Main process SampleJob01
```

Instead of `SampleJob01`use the identifier for your job you created.

#### Cleaning up status for a job
Just in case you want to re-reun ingestion for a job with the same name/identifier, it is possible to also delete all metadata documents for a job.

```
java -cp ./azure-cosmos-distributed-bulk-sample-1.0-SNAPSHOT-jar-with-dependencies.jar com.azure.cosmos.samples.distributedbulk.Main delete SampleJob01
```

Afterwards you can craete the job with the same name.

### Very high level design description
For each batch (chunk of an input file) some context is captured and will be passed through the lifecycle of the ingestion. The chunk of documents to be processed is stored in a `Batch`. Processing the batch is done by invoking the correct operation in `DocumentBulkExecutor` to upsert/insert/delete etc. For each method invocation - tracking the ingestion of all documents of this batch an is captured in `DocumentBulkExecutorOperationStatus`. Here status like the number of successfully ingested documents, the number of outstanding operations, any failures (exceeding the defined retry count) etc. is captured.
Each worker will process a configurable (see `COSMOS.MAX_CONCURRENT_BATCHES_PER_MACHINE`) number of batches - the higher, the more documents can be processed per machine - as long as the machine is not overloaded (monitor CPU/memory).




