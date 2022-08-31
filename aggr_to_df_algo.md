## Algorithm used in `aggr_to_df`

First phase is updating the cache. A single user's cache contains: `info` - `DataFrame` containing all the available info about the user, `img_path` - the path to their latest image, `last_modified` - time of the last modification of user's info file in the database.

A lexicographically sorted list of the available user data objects is retrieved from the database, and is iterated over. Since it is lexicographically sorted, all files belonging to a given user will be provided in a contiguous fragment. Let's focus on what happens during processing of a single user in this phase.

For the current user, we keep their cached info stored in `user_dict`, the previously processed object's user ID `prev_id`, and a path of the users image `img_path`.
In the given data, images with `.png` extension come after the `.csv` file, however file formats are subject to change, so I chose not to assume the order of files within a single user's files.

After the extraction of the user's ID `user_id` and the file extension `ext`, we check if a new user's files have begun by checking the condition `user_id != prev_id`.

1. `user_id != prev_id` is `True`:

We have to finish processing the user `prev_id` (if they exist, by updating their image's path, stored in `user_dict['img_path']`), and for the new user - reset variables: `prev_id`, `user_dict`, `img_path`.

2. `user_id != prev_id` is `False`:

There is nothing to be done here.

To better visualize the cases, let's take a look at the below:
![](https://i.imgur.com/0Ot6q8L.png)

Cases A, C, D fall into 1. case and case B falls into 2. case. Notice, that this illustrates that an image might be found earlier than the info file. In order to properly handle case D, there's an additional `if` after the loop.

Then, if the file is an image, the `img_path` needs to be updated. If the file is a CSV, then
1. If the cached data is up to date, the current user is added to the list of users who can be processed immediately after this loop.
2. If the cached data is outdated, then the asynchronous download of the current data gets submitted for execution, and the user will be processed after their data is retrieved.

Next phase is filtering the users whose cache was up to date. For that, we update the `img_path` column in their `info` `DataFrame` with their latest image path (possibly empty), and run filters on those info. If the user's info pass filters, their `DataFrame` is added to those to be concatenated into one aggregated output `DataFrame`.

The last major phase is processing those users whose data was to be fetched from the database. We iterate over the submitted jobs in order of their completion, retrieve user's ID based on the currently processed job, parse the downloaded CSV file into a `DataFrame`, format it to contain all the available info, and lastly apply filters to the user's info just like in the case of cached users.

Finally, returned is a `DataFrame` consisting of the columns passed as an argument and whose rows contain info of users matching the given filters.
