
# Base data retriever

This service is responsible for retrieving data from a normalized database, in
a UNIT-TIME long form format. The service acts as the translator between these
two data formats.

It does so by reflecting the relational structure of the database to figure out
how to join tables, in order to produce the desired output format.

Choose the kind of join via the `OUTER_JOINS` setting. Setting this to `True`
ensures that only outer joins are used, which means that all time-unit
combinations at the desired level of analysis are returned.

## Env settings

| Key                                  | Description                                                    | Default                         |
|--------------------------------------|----------------------------------------------------------------|---------------------------------|
| BASE_DATA_RETRIEVER_OUTER_JOINS      | Should joins from the LOA be outer left joins?                 | False (use inner joins)         |
| BASE_DATA_RETRIEVER_DB_HOST          | Hostname of database containing data to retrieve               | 127.0.0.1                       |
| BASE_DATA_RETRIEVER_DB_PORT          | Port of -"-                                                    | 5432                            |
| BASE_DATA_RETRIEVER_DB_USER          | Username for -"-                                               | postgres                        |
| BASE_DATA_RETRIEVER_DB_NAME          | Dbname for -"-                                                 | postgres                        |
| BASE_DATA_RETRIEVER_DB_PASSWORD      | Password for -"-                                               | None                            |
| BASE_DATA_RETRIEVER_DB_SCHEMA        | Schema to search for data in -"-                               | None                            |
| BASE_DATA_RETRIEVER_DB_SSLMODE       | SSLmode for connection to -"-                                  | allow                           |
| BASE_DATA_RETRIEVER_LOA_DB_HOST      | Hostname of database containing LOA definitions                | BASE_DATA_RETRIEVER_DB_HOST     |
| BASE_DATA_RETRIEVER_LOA_DB_PORT      | Port of -"-                                                    | BASE_DATA_RETRIEVER_DB_PORT     |
| BASE_DATA_RETRIEVER_LOA_DB_USER      | Username for -"-                                               | BASE_DATA_RETRIEVER_DB_USER     |
| BASE_DATA_RETRIEVER_LOA_DB_NAME      | Dbname for -"-                                                 | BASE_DATA_RETRIEVER_DB_NAME     |
| BASE_DATA_RETRIEVER_LOA_DB_PASSWORD  | Password for -"-                                               | BASE_DATA_RETRIEVER_DB_PASSWORD |
| BASE_DATA_RETRIEVER_LOA_DB_SCHEMA    | Schema to search for data in -"-                               | public                          |
| BASE_DATA_RETRIEVER_LOA_DB_SSLMODE   | SSLmode for connection to -"-                                  | BASE_DATA_RETRIEVER_DB_SSLMODE  |
| LOG_LEVEL                            | Python logging level                                           | WARNING                         |

In addition, all settings inherited from [uvicorn_deployment](https://github.com/prio-data/uvicorn_deployment).

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
