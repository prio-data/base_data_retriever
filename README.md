
# Base data retriever

This service is responsible for retrieving data from a normalized database, in
a UNIT-TIME long form format. The service acts as the translator between these
two data formats.

It does so by reflecting the relational structure of the database to figure out
how to join tables, in order to produce the desired output format.

## Env settings

|Key                             |Description                                                   |Default                |
|--------------------------------|--------------------------------------------------------------|-----------------------|
|BASE_DB_HOST                    |Hostname of database containing data to retrieve              |127.0.0.1              |
|BASE_DB_PORT                    |Port of -"-                                                   |5432                   |
|BASE_DB_USER                    |Username for -"-                                              |postgres               |
|BASE_DB_NAME                    |Dbname for -"-                                                |postgres               |
|BASE_DB_PASSWORD: Optional[str] |Password for -"-                                              |None                   |
|BASE_DB_SCHEMA: Optional[str]   |Schema to search for data in -"-                              |None                   |
|BASE_DB_SSLMODE                 |SSLmode for connection to -"-                                 |allow                  |
|LOA_DB_HOST                     |Hostname of database containing LOA definitions               |BASE_DB_HOST           |
|LOA_DB_PORT                     |Port of -"-                                                   |BASE_DB_PORT           |
|LOA_DB_USER                     |Username for -"-                                              |BASE_DB_USER           |
|LOA_DB_NAME                     |Dbname for -"-                                                |BASE_DB_NAME           |
|LOA_DB_PASSWORD: Optional[str]  |Password for -"-                                              |BASE_DB_PASSWORD       |
|LOA_DB_SCHEMA                   |Schema to search for data in -"-                              |public                 |
|LOA_DB_SSLMODE                  |SSLmode for connection to -"-                                 |BASE_DB_SSLMODE        |
|LOG_LEVEL                       |Python logging level                                          |WARNING                |

In addition, all settings inherited from [uvicorn_deployment](https://github.com/prio-data/uvicorn_deployment).

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
