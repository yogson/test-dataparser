from sys import argv
from urllib import request
from urllib.error import HTTPError
import gzip
import os
import ssl
import json
from datetime import datetime

import psycopg2

from config import URL, DB

# Set SSL environment var to load insecure content
if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
    ssl._create_default_https_context = ssl._create_unverified_context


def download_file(file_name: str) -> list:
    uri = URL + file_name
    req = request.Request(uri)

    try:
        resp = request.urlopen(req)
    except HTTPError:
        return []

    try:
        file_obj = gzip.GzipFile(fileobj=resp)
    except:
        return []

    return file_obj.readlines()


def validate_item(line: dict):
    if not isinstance(line, dict):
        return {'error': 'malformed line format'}, False

    user, ts, context, ip = line.get('user'), line.get('ts'), line.get('context'), line.get('ip')
    if not user and ts and context and ip:
        return {'error': 'incomplete data set'}, False

    if not isinstance(user, int):
        if not user and ts and context and ip:
            return {'error': 'bad user id'}, False

    try:
        ts = datetime.fromtimestamp(ts)
    except Exception as e:
        return {'error': 'bad timestamp'}, False

    if isinstance(context, dict):
        context = json.dumps(context)
    else:
        return {'error': 'bad context'}, False

    if not isinstance(ip, str):
        return {'error': 'bad data in ip'}, False

    return (user, ts, context, ip), True


def parse_file(raw_lines: list) -> tuple:
    parsed, errors = [], []

    for line in raw_lines:
        try:
            loaded_line = json.loads(line)
        except json.JSONDecodeError as e:
            loaded_line = None
            try:
                decoded_line = line.decode()
            except:
                decoded_line = None
            errors.append((
                decoded_line if decoded_line else line,
                'JSON decode error: ' + e.msg
            ))

        if loaded_line:
            prepared_item, success = validate_item(loaded_line)

            if success:
                parsed.append(prepared_item)
            else:
                errors.append((
                    loaded_line,
                    prepared_item.get('error')
                ))

    return parsed, errors


def insert_data(data: list, errors: list, report_params: dict):
    conn_string = f'host={DB.get("host")} dbname= {DB.get("dbname")} user={DB.get("user")} password= {DB.get("password")}'

    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cursor:

            if data:
                template = ','.join(['%s'] * len(data))
                insert_query = 'INSERT INTO {table} ("user", ts, context, ip) VALUES {template}'.format(
                    table=DB.get('report_table'),
                    template=template
                )
                cursor.execute(insert_query, data)

            if errors:
                errors = [
                    (
                        report_params.get('api_report'),
                        report_params.get('api_date'),
                        *err_item,
                        datetime.now()
                    ) for err_item in errors
                ]
                template = ','.join(['%s'] * len(errors))
                insert_query = 'INSERT INTO {table} (api_report, api_date, row_text, error_text, ins_ts) VALUES {template}'.format(
                    table=DB.get('error_table'),
                    template=template
                )
                cursor.execute(insert_query, errors)


if __name__ == '__main__':

    if len(argv) != 2:
        print('Usage: python3 main.py <report_file_name>')

    else:

        file_name = argv[1]
        api_report, *api_date = file_name.split('.')[0].split('-')

        report_file = download_file(file_name)

        if report_file:

            parsed, errors = parse_file(report_file)
            insert_data(
                parsed,
                errors,
                {
                    'api_report': api_report,
                    'api_date': datetime.strptime('-'.join(api_date), '%Y-%m-%d')
                }
            )
            print('Success!')

        else:
            print('Unable to load report.')

