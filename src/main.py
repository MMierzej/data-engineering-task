from datetime import datetime as dt, timezone as tz

from flask import Flask, jsonify, request
from minio import Minio

from dthelpers import *
from udt import UserDataTransformer as UDT


def convert_to_bool(param):
    """
    Takes a string parameter `param` and converts it to a boolean value.
    The `param` must be either 'True' or 'False', else a `ValueError` is raised.
    """
    if param not in ('True', 'False'):
        raise ValueError('Boolean parameter must be "True" or "False"')
    return param == 'True'


def convert_to_nonneg_float(param):
    """
    Takes a string parameter `param` and converts it to a non-negative float
    value. The `param` must be convertible to such value using `float(param)`,
    else a `ValueError` is raised.
    """
    param = float(param)
    if param < 0:
        raise ValueError(
            'Given float is negative, expecting non-negative float')
    return param


def get_params_vals(req_args):
    """
    Constructs a dictionary mapping parameter names from `GET_params`
    dictionary to values provided for them in `req_args` dictionary,
    using the defined in `GET_params` converters for each parameter.
    """
    return {name: conv(val) for name, conv in GET_params.items()
            if (val := req_args.get(name)) is not None}


def params_to_filters(params):
    """
    Constructs filters to be applied to user data. It does that
    by creating a list of pairs of the format: `('column_name', filter_function)`,
    where `filter_function` returns a boolean value based on the value from the
    column named `column_name`. The expected behavior of filtering user data
    is that a user should be filtered out if any of the filters evaluates to `False`.
    """
    filters = []
    now = dt.now(tz.utc)

    if 'min_age' in params:
        try:
            max_birthts = timestamp_from_age(now, params['min_age'])
        except:
            max_birthts = dt_to_millis(MINDATETIME)
        filters.append(('birthts', lambda ts: ts <= max_birthts))

    if 'max_age' in params:
        try:
            min_birthts = timestamp_from_age(now, params['max_age'])
        except:
            min_birthts = dt_to_millis(MINDATETIME)
        filters.append(('birthts', lambda ts: ts >= min_birthts))

    if 'image_exists' in params:
        filters.append(('img_path', lambda p:
                        (p != udt.empty_value) == params['image_exists']))

    return filters


app = Flask(__name__)


@app.route('/data', methods=['GET', 'POST'])
def data():
    """
    The `/data` endpoints of the server.

    The `GET` method version takes the filter parameters: `image_exists`,
    `min_age`, `max_age`, and in JSON format returns a list of users (and their
    corresponding data) matching those given filters, who are present in the database
    at the time of the request being received.

    The `POST` method version aggregates the data of all users present in the
    database into a single CSV file and stores this file in the database under
    the name `processed_data/output.csv`.

    Should an exception occur during handling the request, its string value
    is returned as a response.
    """
    if request.method == 'GET':
        try:
            params = get_params_vals(request.args)
            filters = params_to_filters(params)

            # constructing a JSON response with empty data, then
            # updating it with the target data
            resp = jsonify()  
            resp.set_data(udt.aggr_user_data(filters=filters,
                                             out_format={'format': 'json',
                                                         'bin': True}))
            return resp
        except Exception as e:
            return str(e)

    else:  # POST method
        try:
            write_result = udt.update_output(output_csv_name)
            return str(write_result)
        except Exception as e:
            return str(e)


@app.route('/stats', methods=['GET'])
def stats():
    """
    The `GET /stats` endpoint of the server.

    Given filter parameters: `image_exists`, `min_age`, `max_age`,
    in the `GET` request's query string, the endpoint returns as JSON
    the average age of users matching the given filters, who are present
    in the database.
    """
    try:
        params = get_params_vals(request.args)
        filters = params_to_filters(params)
        return jsonify(udt.avg_user_age(filters=filters))
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    # server setup

    # database client
    mc = Minio(
        'minio:9000',
        access_key='admin',
        secret_key='password',
        secure=False
    )

    bucket_name = 'datalake'

    # directory in which the source data is stored in the bucket
    src_dir = 'source_data/'

    # columns to be included in the output CSV aggregating user data
    dflt_out_columns = ['user_id', 'first_name',
                        'last_name', 'birthts', 'img_path']
    
    # the output CSV's field delimiter
    delimiter = ', '

    output_csv_name = 'processed_data/output.csv'

    # the object which helps the server interface with the database
    # and is able to perform all the needed data transformations
    udt = UDT(
        mc,
        bucket_name,
        out_columns=dflt_out_columns,
        src_dir=src_dir,
        csv_delim=delimiter
    )

    # a dictionary defining the filter parameters to be extracted
    # from the `GET` request's query string, and is of the format:
    # 'param_name': converter
    GET_params = {
        'image_exists': convert_to_bool,
        'min_age': convert_to_nonneg_float,
        'max_age': convert_to_nonneg_float,
    }

    app.run(host='0.0.0.0', port=8080)
