from collections import defaultdict as dd
import concurrent.futures as cf
from datetime import datetime as dt, timezone as tz
from io import BytesIO
from os.path import splitext
from threading import Lock

import pandas as pd

from dthelpers import *


class UserDataTransformer:
    """
    `UserDataTransformer` objects are to be used to interface with
    the storage containing users' data, perform filtration and
    aggregation of information present in the storage, export
    the results to a specific format, and return them or upload
    them directly to the database.
    """

    def __init__(self, storage_client, bucket_name, out_columns,
                 src_dir='', csv_delim=',', empty_value=chr(248), img_exts=None):
        """
        `storage_client` Client of the database to draw data from.

        `bucket_name`    Name of the bucket in which the data is stored.

        `out_columns`    List of columns (their names) to be used as the default format
                         of the exported CSV.

        `src_dir`        Name of the directory in which the data is stored
                         in the bucket.

        `csv_delim`      Field delimiter used in the input CSVs.

        `empty_value`    A value to use in the output CSV to signify an empty cell.
                         It's useful when the delimiter contains a trailing whitespace,
                         to avoid troubles with parsing the CSV back after exporting.

        `img_exts`       A set of extensions of files to be recognized as images.
        """
        self.client = storage_client
        self.bucket_name = bucket_name
        self.dflt_out_columns = out_columns
        self.src_dir = src_dir
        self.csv_delim = csv_delim
        self.empty_value = empty_value
        self.img_exts = img_exts or {'.png'}

        # since a server which is using this object might handle requests
        # in multiple threads, a couple of them might want to modify the cache
        # at once. the lock will be used to prevent this, as concurrent modification
        # could lead to the corruption of locally stored data
        self._lock = Lock()
        self._user_cache = dd(lambda: {
            'info': None,
            'img_path': self.empty_value,
            'last_mod': MINDATETIME,
        })

    def _uid_n_ext(self, path):
        """
        Given the file path `path`, extracts `user_id` of the user whom
        the file belongs to, and the extension of the file.
        """
        base, ext = splitext(path)
        user_id = base[base.rfind('/') + 1:]
        return user_id, ext

    def _read_csv(self, bin_data, delimiter):
        """
        Reads binary data `bin_data` of a CSV into a `pandas.DataFrame`,
        using the given `delimiter`. The delimiter can be multicharactered.
        """
        return pd.read_csv(BytesIO(bin_data),
                           delimiter=delimiter,
                           engine='python')     # enables multicharactered delimiters

    def export_df(self, df, format='csv', delimiter=',', bin=True):
        """
        Export a `DataFrame` `df` to a given `format` (currently
        'csv' or 'json', other formats will cause `ValueError`).
        If `bool(bin) == True`, the output will be binary, otherwise - a string.
        """
        if format == 'csv':
            output_data = df.to_csv(index=False)
            if delimiter != ',':
                output_data = output_data.replace(',', delimiter)
        elif format == 'json':
            output_data = df.to_json(orient='records',
                                     force_ascii=False,
                                     indent=2)
        else:
            raise ValueError(f'Unsupported output data format: {format}')
        return output_data.encode('utf-8') if bin else output_data

    def aggr_to_df(self, out_columns=None, filters=None, img_exts=None):
        """
        Aggregates users' filtered data, stored currently in the database,
        into a `DataFrame`.

        `out_columns`   List of columns (their names) that the resulting `DataFrame`
                        will consist of. If `out_columns` is not given or is `None`,
                        the result will consist of the default columns given in the
                        constructor.

        `filters`       List of pairs of the format: `('column', filter_function)`, where
                        `filter_function` is to be applied to the value in the `column`
                        in a given user's data. For a given user, if any of the filters
                        returns a value whose boolean interpretation is `False`, then
                        information about this user will not be included in the result.
                        If `filters` is not given or is `None`, then no filter will be
                        applied.

        `img_exts`      A set of file extensions that will be used to identify image files
                        by their extension. If `img_exts` is not given or is `None`, images
                        will be identified based on the `img_exts` passed to the constructor.
        """
        if out_columns is None:
            out_columns = self.dflt_out_columns
        if filters is None:
            filters = []
        if img_exts is None:
            img_exts = self.img_exts

        def process_user(user_id):
            """
            Updates the `img_path` and checks if the user matches the given filters.
            Returns user's info contained in a `DataFrame` if the user matches filters,
            or `None` otherwise.
            """
            user_dict = self._user_cache[user_id]
            user_info = user_dict['info']
            if user_info is None:
                return None
            user_info.at[0, 'img_path'] = user_dict['img_path']

            try:
                if all((check(user_info.at[0, column]) for column, check in filters)):
                    return user_info
                else:
                    return None
            except KeyError:
                raise KeyError(
                    'One of the filters tried to access a non-existing column')
            # the other filter-related exceptions are raised normally

        # using threads to minimize the time spent on downloading data from the database
        # using a lock to prevent the corruption of data caused by uncontrolled concurrent modification
        with cf.ThreadPoolExecutor() as executor, self._lock:
            cached_users = []    # users who can be processed immediately, based on cached data
            future_to_user = {}  # dictionary mapping asynchronous jobs to users

            # the below algorithm is described in the README

            prev_id = ''
            img_path = self.empty_value
            user_dict = None

            # as per: https://github.com/minio/minio-py/issues/775
            # I'm assuming that objects will be given sorted lexicographically by names
            for o in self.client.list_objects(self.bucket_name,
                                              prefix=self.src_dir,
                                              recursive=True):
                user_id, ext = self._uid_n_ext(o.object_name)

                if user_id != prev_id:
                    if prev_id:
                        user_dict['img_path'] = img_path
                    prev_id = user_id
                    img_path = self.empty_value
                    user_dict = self._user_cache[user_id]

                if ext in img_exts:
                    img_path = o.object_name
                elif ext == '.csv':
                    # checking if cached data is outdated
                    if user_dict['last_mod'] < o.last_modified:
                        user_dict['last_mod'] = o.last_modified

                        # submitting downloading for asynchronous execution
                        future = executor.submit(self.client.get_object,
                                                 self.bucket_name,
                                                 o.object_name)
                        future_to_user[future] = user_id
                    else:
                        cached_users.append(user_id)

            if prev_id:
                user_dict['img_path'] = img_path

            # list of DataFrames of cached users whose info satisfies filters
            result = [df for user_id in cached_users
                      if (df := process_user(user_id)) is not None]

            # processing users whose info had to be downloaded
            for future in cf.as_completed(future_to_user):
                user_id = future_to_user[future]
                user_dict = self._user_cache[user_id]

                try:
                    # .result() reraises the exception if it occured during async execution
                    response = future.result()
                    df = self._read_csv(response.data,
                                        delimiter=self.csv_delim)

                    # first column is 'user_id', and 'img_path' is the last one
                    df.insert(0, 'user_id', [user_id])
                    df.insert(len(df.columns),
                              'img_path',
                              [self.empty_value])
                except:
                    df = None
                    user_dict['last_mod'] = MINDATETIME
                finally:
                    response.close()
                    response.release_conn()

                user_dict['info'] = df
                df = process_user(user_id)
                if df is not None:
                    result.append(df)

        if result:
            df = pd.concat(result, ignore_index=True)
            try:
                return df[out_columns]
            except KeyError:
                raise KeyError(
                    'Given output columns contain a name not present in the columns of data')
        else:
            return pd.DataFrame(columns=out_columns)

    def aggr_user_data(self, out_columns=None, filters=None, img_exts=None, out_format=None):
        """
        Aggregates users' filtered data stored currently in the database into a string
        or bytes, of the format specified in the `out_format` dictionary (see below).

        `out_columns`   List of columns (their names) that the result will consist of.
                        If `out_columns` is not given or is `None`, the result will
                        consist of the default columns given in the constructor.

        `filters`       List of pairs of the format: `('column', filter_function)`, where
                        the `filter_function` is to be applied to the value in the `column`
                        in a given user's data. For a given user, if any of the filters
                        returns a value whose boolean interpretation is `False`, then
                        information about this user will not be included in the result.
                        If `filters` is not given or is `None`, then no filter will be
                        applied.

        `img_exts`      A set of file extensions that will be used to identify image files.
                        If `img_exts` is not given or is `None`, images will be identified
                        based on the `img_exts` set passed to the constructor.

        `out_format`    A dictionary containing a (sub)set of the following keys and their
                        corresponding values:
                            - `format`:
                                - `csv`  CSV format
                                - `json` JSON format
                            - `delimiter` (used if `format` is `csv`):
                                a string which will separate fields in a row
                            - `bin`:
                                - `True`  output will be binary
                                - `False` output will be a string,
                        which describes the format of the output.
                        If is `None`, then the format will be CSV in binary, with fields
                        in rows delimited by the delimiter passed to the constructor.
        """
        if out_format is None:
            out_format = {'format': 'csv', 'delimiter': self.csv_delim, 'bin': True}

        return self.export_df(self.aggr_to_df(out_columns, filters, img_exts), **out_format)

    def avg_user_age(self, filters=None, img_exts=None):
        """
        Calculates the average age of users matching the given `filters`,
        whose data is currently stored in the database. In order to calculate the age
        of a user, a column "birthts", containing a user's UTC birthdate timestamp in
        milliseconds from POSIX epoch, must exist in the data. If there are no records
        to calculate the average age from, `-1` is returned.

        `filters`       List of pairs of the format: `('column', filter_function)`, where
                        the `filter_function` is to be applied to the value in the `column`
                        in a given user's data. For a given user, if any of the filters
                        returns a value whose boolean interpretation is `False`, then
                        information about this user will not be included in the result.
                        If `filters` is not given or is `None`, then no filter will be
                        applied.

        `img_exts`      A set of file extensions that will be used to identify image files.
                        If `img_exts` is not given or is `None`, images will be identified
                        based on the `img_exts` set passed to the constructor.
        """
        try:
            df = self.aggr_to_df(['birthts'], filters, img_exts)
        except KeyError:
            raise KeyError(
                'User data do not contain the "birthts" column, unable to calculate age')

        return -1 if df.empty else age_from_timestamp(dt.now(tz.utc), df['birthts'].mean())

    def update_output(self, output_name, out_format=None):
        """
        Aggregates into a format specified in `out_format` dictionary (see below)
        all available data currently stored in the database about users, and uploads
        the result into the database as a file under the name `output_name`.

        `out_format`    A dictionary containing a (sub)set of the following keys and their
                        corresponding values:
                            - `format`:
                                - `csv`  CSV format
                                - `json` JSON format
                            - `delimiter` (used if `format` is `csv`):
                                a string which will separate fields in a row
                            - `bin`:
                                - `True`  output will be binary
                                - `False` output will be a string,
                        which describes the format of the output.
                        If is `None`, then the format will be CSV in binary, with fields
                        in rows delimited by the delimiter passed to the constructor.
        """
        output_bytes = self.aggr_user_data(out_format=out_format)
        return self.client.put_object(self.bucket_name,
                                      output_name,
                                      data=BytesIO(output_bytes),
                                      length=len(output_bytes),
                                      content_type='application/csv')
