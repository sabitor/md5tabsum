# Table checksum calculation - md5tabsum
The purpose of the tool **md5tabsum** is to calculate the MD5 checksum from the content of database tables. 
The MD5 calculation is performed in the DBMS. Finally, the MD5 checksum of the table is sent back to the application, where a key-value pair is created and output to STDOUT as follows:
```
exasol.prod.TAB2:71d6a96d8a73ab1de03ac9f587d54bdf
exasol.dev.TAB1:ee509260309b18c91a931fc77920f631
exasol.test.TAB3:71d6a96d8a73ab1de03ac9f587d54bdf
...
```
The key (the key-value separator is the : character) specifies the ID of the table used to calculate the checksum (for example table TAB2 in Exasol prod) and the value specifies the calculated MD5 checksum of this table.

Currently, the following DBMS are supported:
- Exasol
- MS SQL Server
- MySQL
- Oracle
- PostgreSQL

**Hint:** The tool can also be used to compare tables accross DBMS boundaries. This means, tables with the same content and structure and stored in different DBMS have the same checksum.

## Use cases
Why would it make sense to calculate a checksum of a database table? What is the added value? The following scenarios are conceivable (there may be more), for example:
1. Database migration - Verification that all data was initially transferred correctly from the old system to the new system
2. Database benchmarking - Signature of the used data to enable identical initial conditions 
3. Maintaining cache consistency - Maintaining a data cache (table) to know if the cache is still valid or needs to be refreshed

## How to configure
How md5tabsum works is determined by a configuration file in [YAML](https://yaml.org) format. This file can have any name and is passed as an argument when calling md5tabsum.

The configuration file consists of two sections:
- Common section
- DBMS instance section

As the name implies, the *common section* contains key-value pairs which are valid for all configured instances. A list of all common keywords including their details can be found in the following table:
Common Keyword | Value | Comments
--- | --- | ---
Logfile | full qualified name of the log file | The specified name has to conform to the OS file name convention. This config file parameter is mandatory.
Passwordstore | full qualified name of the password store | The specified name has to conform to the OS file name convention. This config file parameter is mandatory.

The *DBMS instance section* can consist of one or multiple so-called *DBMS instances*. These are delimited sections for dedicated DBMS, host, users and tables. 
A DBMS instance section is structured as follows:
```
Predefined DBMS-Name:
  Instance ID:
    Keyword1: Value
    ...
    KeywordN: Value
```
The *Predefined DBMS-Name* is a fixed name that designates both the first row of an instance section and the DBMS in which the tables to be checked reside. The following predefined names are valid:
- Exasol
- Mssql
- Mysql
- Oracle
- Postgresql

<p></p>
In addition, the next level of an instance section is an identifier, that uniquely identifies the instance associated with a DBMS. This identifier can consist of any ASCII characters.

In addition, multiple instances can be assigned to a DBMS if, for example, tables that are in different database environments of the same DBMS type have to be compared. To do this, assign a different instance part to the relevant DBMS, which can be uniquely identified by its instance ID, e.g.:
```
Predefined DBMS-Name:
  Instance ID 1:
    Keyword1: Value
    ...
    KeywordN: Value
  
  Instance ID 2:
    Keyword1: Value
    ...
    KeywordN: Value
```

**Hint:** This tool supports parallelism to calculate table checksums. The degree of parallelism is defined by the number of active DBMS instances. This means that each active DBMS instance starts a dedicated DBMS session in which the checksums are calculated.

<p></p>
Finally, there are required key-value pairs per instance. They are the properties of an instance and contain all connection details and the corresponding tables to be used for the checksum calculation.
<p></p>
A list of all supported keywords including their details can be found in the following table:
<p></p>

Instance Keyword | Value | Comments
--- | --- | ---
Active | 0 or 1 | Set to 1 uses this instance, set to 0 this instance is skipped. It helps temporarily disable or enable an instance from being considered. This config file parameter is optional. If not set it defaults to 0.
LogLevel | 0 or 1 or 2 | Specifies the level of detail of the log output, where 0 means no logging, 1 means simple logging, and 2 means extended logging. This config file parameter is optional. If not set it defaults to 0.
Host | DNS name or IP address | This config file parameter is mandatory.
Port | port number | This config file parameter is mandatory.
User | user name | This config file parameter is mandatory.
Database | database name | This is only required for SQL Server and PostgreSQL, where it is mandatory.
Service | service name | This is only required for Oracle, where it is mandatory.
Schema | schema name | This config file parameter is mandatory.
Table | single table or comma separated list of tables including placeholder characters (%) | This config file parameter is mandatory.

### Example
 Suppose you want to calculate the checksum for a few tables in an Exasol database running in a test environment. The following properties are given:
 - Host name is testserver1.company.com
 - Port number is 8563
 - User name is user123
 - Schema name is emea
 - Table name contains the substring TEST
 - Table name is equal to EMPLOYEES
 - Table name starts with the prefix TA
  
Based on these requirements the instance section in the configuration file looks like this:

 ```
 Exasol:
  Test1:
    Active:   1
    Loglevel: 1
    Host:     testserver1.company.com
    Port:     8563
    User:     user123
    Schema:   emea
    Table:    \%_TEST_%, EMPLOYEES, TA%
 ```
**Hint:** If the first character in a config file value is a special characters such as '%', it has to be preceded by a '\\' character to avoid config file parsing errors. 

## How to run
To get an overview of all command options and how to run the tool you can invoke the following command:
```
md5tabsum -h
```
which outputs the following details:
```
Usage of ./md5tabsum:
  -c string
        config file name (default "md5tabsum.cfg")
  -i string
        instance name
          The defined format is <DBMS>.<instance ID>
  -p string
        password store action
          create - creates the password store based on the instances stored in the config file
          add    - adds a specific instance and its password in the password store
          update - updates the password of a specific instance in the password store
          delete - deletes a specific instance and its password from the password store
          show   - shows all stored instances in the password store
  -v    version information
```
Before the calculation of the table checksum can be started for the first time, some setup requirements must be met:
1. The configuration file has to be created. What needs to be considered there can be found in chapter *How to configure* above.
2. The instance passwords have to be written to the so called *password store* - a file where the instance passwords are stored using AES encryption. To do this, the following command has to be triggered, in which you will be asked for the user password for all active instances:
```
md5tabsum -c <config file> -p create
Enter password for instance exasol.test:
```
**HINT:** While entering the password it is not printed on STDOUT.

After all setup requirements have been met, the checksum calculation can be started as follows:
```
md5tabsum -c <config file name>
```
If the execution completed successfully, the generated output written to STDOUT might look like this:
```
exasol.test1.HASH_TEST_A:f695eef8946454712aaf36c5489e6b0e
exasol.test1.HASH_TEST_B:d3f4d75f5bc9b09a740e93039c8fd132
exasol.test1.EMPLOYEES:ea30f02b2d119e66dc25783f0b4e9bce
exasol.test1.TAB3:71d6a96d8a73ab1de03ac9f587d54bdf
exasol.test1.TAB2:71d6a96d8a73ab1de03ac9f587d54bdf
```
If there would be issues with one of the configured DBMS instances you wouldn't find a result key-value pair of that instance in the output.
In such cases an error message is written to STDOUT and to the md5tabsum log file. 

For example, if there would be some authentication issues with the configured DBMS, the message sent to STDOUT could be:
```
E-EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - authentication failed.'
```
As a result of this error, the configured username and saved password should be validated.
