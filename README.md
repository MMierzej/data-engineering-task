# Data Engineering Task


## Introduction

This repository contains a solution authored by Marcin Mierzejewski to the test assignment for Data Engineering Internship at Provectus. It is implemented in Python, using [Flask](https://flask.palletsprojects.com/en/2.2.x/) for the server, and [MinIO](https://min.io/) for the database. It leverages Docker containers defined in this [`docker-compose.yml`](./buildconfig/docker-compose.yml) file to attain portability. The implemented service can be built and run automatically with [`run.sh`](./run.sh) script.


## Task


### Brief description

Implement a service with available endpoints, which performs operations on data about users, stored in a remote database, such as answering queries about the data, and aggregating the data into an output, which may be uploaded to the database.

The full statement can be found in [`task.md`](./task.md).


## Solution


### Initial analysis

In my approach I endeavored to make the solution as general as I could. Questions I have asked myself were, among others:
- Do user IDs have to be integer numbers?
- Can there be stored a user's image, but not this user's info?
- Can user data be modified in or deleted from the database?
- Can I make the server retrieve only the necessary updates instead of downloading everything everytime?
- What if the format of input data is changed?
- What if we needed different filters than those specified?
- What if in the output we wanted to have only a subset of all available columns?
- What if there is a lot of user data?

Considering those questions lead me to several design decisions, namely:
- Creation of the `UserDataTransformer` class which provides an interface with the database containing user data, and is able to perform generalized versions of the operations required in the task.
- `UserDataTransformer`'s methods take arguments, such as custom formats, filters, etc. This makes them highly flexible, and easily adaptable to changes.
- Caching the downloaded data - only the modifications since the last database request are retrieved, the rest is already stored locally. This makes for an efficient use of the network and time.
- Using threads to asynchronously download data from the database, allowing their parallel retrieval and being able to do work in the meantime.

The technical details are described below in the *Implementation* section.


### Implementation

#### `UserDataTransformer` class

The `UserDataTransformer`'s code resides in [`udt.py`](./src/udt.py), and its methods is where all the most important work is done.

##### `__init__`

Takes all the necessary arguments for a `UserDataTransformer` object to function properly, such as the database client and default values to be used. All the details are described in the constructor's docstring.

##### `aggr_to_df`

The most crucial of the methods is `aggr_to_df`, used by the other methods, as it aggregates and filters all the available user data into a [`pandas.DataFrame`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) following the output format and using filters specified as arguments. This `DataFrame` can then be further processed by the other methods.

As the other technical details are described in the method's docstring and code comments, in [`aggr_to_df_algo.md`](./aggr_to_df_algo.md) is an explanation of the used algorithm.

##### `export_df`

Exports the given `DataFrame` to a specified format.

##### `aggr_user_data`

Uses `aggr_to_df` and `export_df` to aggregate, filter, and export the user data.

##### `avg_user_age`

Firstly, aggregates and filters users' data using `aggr_to_df`, then extracts and returns the average age of the received users, with help of `pandas.DataFrame` methods (if there are no users matching filters, `-1` is returned).

##### `update_output`

Uses `aggr_user_data` to generate the CSV encoded in binary, containing all the available user data in the columns specified in `out_format`, and using the database client uploads the CSV as a proper file.

#### Server

The server is implemented using [Flask](https://flask.palletsprojects.com/en/2.2.x/), answers on `localhost:8080`, and makes the following endpoints available (their functionality is described in the [task's statement](./task.md)):
- `GET /data`
- `POST /data`
- `GET /stats`

In the server's code in [`main.py`](./src/main.py), the routes and their corresponding functions are established. These functions are simple themselves, as they use `UserDataTransformer`'s methods to perform all the heavier work. They also use helper functions (with detailed descriptions written in their docstrings) - converters which convert query string parameters to a list of the actual filters to be applied to user data.
If a specific filter parameter is not provided, then the corresponding filter is not applied (for example, if in the query string there is no `image_exists`, then users are aggregated regardless of them having an image or not). If parameter values are incorrect, the appropriate error message is returned.
Below the routing, there is a general setup section, which creates a database client to be used, useful variables, and finally runs the server itself.

The periodic updating of `processed_data/output.csv` is achieved by scheduling a [`cron`](https://en.wikipedia.org/wiki/Cron) job - a [`curl`](https://curl.se/) [command](./buildconfig/server/sysconf/crontab) performing a `POST http://localhost:8080/data` request, triggering the aggregation and upload of all the available user data in CSV format.

#### Service

The service runs with a single [`run.sh`](./run.sh) script execution as a set of containers defined in [`docker-compose.yml`](./buildconfig/docker-compose.yml). These are:
- [MinIO](https://min.io/) server container
- a container creating the [MinIO](https://min.io/) `datalake` bucket and populating its `source_data` folder with the contents of the [`data`](./data) folder
- my server's container, which firstly starts [`cron`](https://en.wikipedia.org/wiki/Cron) to schedule periodic updating of `processed_data/output.csv` (the interval is easily adjustable, currently set to 1 minute for demonstration), and then runs the server's code as the main process


### Scalability

There are a couple of factors which make my solution scalable.

#### General-purpose methods

I endeavored to not rely too heavily on the current formats of data in the project. Methods of the `UserDataTransfomer` class, which handle all the data transformation, are written so that they are highly flexible in terms of their usage. Customizable are filters and output formats, which both may change throughout the lifecycle of a project.

#### Use of threads

Threads are a great tool for speeding up tasks containing IO-heavy operations, and downloading and processing data from the database certainly are among them. Threads allow work to be done while the data is downloaded, as opposed to waiting for each file to be downloaded before moving on. There were less than 200 files provided in the archive, but it is easy to imagine a real-world application having orders of magnitude more data.

#### Use of caching

Downloading the whole database each time the server receives a request would be a major waste of resources (most importantly - time), especially when only a fraction of files has actually changed since the last database fetch. Caching is a great solution to that problem, provided that the server's memory can store all the files necessary for the handling of requests. If that is not the case, if needed, a policy of cache management can be implemented.

#### Portability

Leveraging Docker containers is a great way to achieve portability - useful in many dimensions.
