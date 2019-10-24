import base64
from datetime import datetime
import fastavro
from google.cloud import storage
import gzip
import json
import urllib


def avro_to_rawls(request):
    start_time = str(datetime.now())

    if (request.is_json):
        request_json = request.json
    else:
        return handle_exception("error", start_time, "Request made at " + start_time + " did not contain application/json payload. Request data: " + request.data)

    try:
        job_id = request_json['jobId']
    except:
        return handle_exception("error", start_time, "Could not find a jobId in the following request made at " + start_time + ": " + str(request.json))

    try:
        url = request_json['url']
        user = request_json['user']
        user_email = user['userEmail']
        user_subject_id = user['userSubjectId']
        workspace = request_json['workspace']
        workspace_name = workspace['name']
        workspace_namespace = workspace['namespace']
    except KeyError as ke:
        return handle_exception(job_id, start_time, "Key Error: " + str(ke) + " in request " + str(request.json))

    defaults = {'b64-decode-enums': True, 'prefix-object-ids': True}
    request_options = request_json.get('options', {})
    options = {**defaults, **request_options}

    try:
        avro = urllib.request.urlopen(url)
    except urllib.error.URLError as ue:
        return handle_exception(job_id, start_time, "URL Error - the url " + url + " is not valid.")

    try:
        reader = fastavro.reader(avro)
        translation = translate(reader, options)
        metadata = {
            "namespace": workspace_namespace,
            "name": workspace_name,
            "userSubjectId": user_subject_id,
            "userEmail": user_email,
            "jobId": job_id,
            "startTime": start_time,
        }
        metadata_json_str = str(json.dumps(metadata))
        metadat_file_name = job_id + "/metadata.json"
        upsert_json_str = str(json.dumps(translation))
        upsert_file_name = job_id + "/upsert.json"
        write_to_bucket(metadat_file_name, metadata_json_str)
        write_to_bucket(upsert_file_name, upsert_json_str)
    except Exception as e:
        return handle_exception(job_id, start_time, "The following exception occurred: " + str(e))
    except:
        return handle_exception(job_id, start_time, "Something went wrong with the following request: " + str(request.json))


def handle_exception(subdirectory_name, start_time, message):
    error_file_name = subdirectory_name + "/error-" + start_time + ".txt"
    write_to_bucket(error_file_name, message)
    return message


def write_upsert_to_bucket(job_id, content_string):
    compressed_value = gzip.compress(bytes(content_string, 'utf-8'))
    file_name = job_id + "/upsert.json"
    write_to_bucket(file_name, compressed_value)


def write_metadata_to_bucket(job_id, content_string):
    file_name = job_id + "/metadata.json"
    write_to_bucket(file_name, content_string)


def write_to_bucket(file_name, content):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("avro-translated-json")
    blob = bucket.blob(file_name)
    blob.upload_from_string(content)


def translate(reader, options=None):
    if options is None:
        options = {}
    t = Translator(options)
    return t.translate(reader)


class Translator:
    def __init__(self, options=None):
        if options is None:
            options = {}
        defaults = {'b64-decode-enums': False, 'prefix-object-ids': False}
        self.options = {**defaults, **options}

    def translate(self, reader):
        if reader is None:
            return None

        enums = _list_enums(reader.writer_schema)
        results = [self._translate_record(record, enums)
                   for record in reader if record['name'] != 'Metadata']
        return results

    def _translate_record(self, record, enums):
        entity_type = record['name']
        name = record['id']

        def make_op(key, value):
            if self.options['b64-decode-enums'] and (entity_type, key) in enums:
                value = _b64_decode(value).decode("utf-8")
            if self.options['prefix-object-ids'] and key == 'object_id':
                value = 'drs://' + value
            return _make_add_update_op(key, value)

        attributes = [make_op(key, value)
                      for key, value in record['object'].items() if value is not None]
        relations = [make_op(relation['dst_name'],
                             {'entityType': relation['dst_name'], 'entityName': relation['dst_id']})
                     for relation in record['relations']]

        return {
            'name': name,
            'entityType': entity_type,
            'operations': [*attributes, *relations]
        }


def _b64_decode(encoded_value):
    return base64.b64decode(encoded_value + "=" * (-len(encoded_value) % 4))


def _list_enums(schema):
    object_field = next(f for f in schema['fields'] if f['name'] == 'object')
    types = [t for t in object_field['type'] if t['name'] != 'Metadata']
    enums = {(entity_type['name'], field['name'])
             for entity_type in types
             for field in entity_type['fields']
             for enum in field['type'] if isinstance(enum, dict) and enum['type'] == 'enum'}
    return enums


def _make_add_update_op(key, value):
    return {
        'op': 'AddUpdateAttribute',
        'attributeName': key,
        'addUpdateAttribute': value
    }