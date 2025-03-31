from flask import Blueprint, Response
import json
from SPARQLBurger.SPARQLQueryBuilder import SPARQLSelectQuery
from website.db import get_scraper_definition, decrypt, generate_airtable_schema
import requests
from ZellijData.AirTableConnection import AirTableConnection
from pyairtable.formulas import match
import functools
from website.transformers.SparqlTransformer import SparqlTransformer
import website.utils.utils as utils
from pyairtable.formulas import EQUAL, OR, STR_VALUE
from werkzeug.wsgi import FileWrapper
import csv
import io


bp = Blueprint('qa', __name__, url_prefix='/qa')

@bp.route("/<api_key>/<model>/<model_id>/<field_id>", methods=["GET"])
def return_execute_qa(api_key, model, model_id, field_id):
    json_data, status = utils.execute_qa(api_key, model, model_id, field_id)

    return Response(json_data, status=status, mimetype='application/json')

@bp.route("/collection/count/<api_key>/<model>/<model_id>/<field_ids>", methods=["GET"])
def return_collection_count(api_key, model, model_id, field_ids):
    json_data, status = utils.count_collection(api_key, model, model_id, field_ids)

    return Response(json_data, status=status, mimetype='application/json')

@bp.route("/collection/sample/<api_key>/<model>/<model_id>/<field_ids>/<skip>", methods=["GET"])
def return_collection_sample(api_key, model, model_id, field_ids, skip):
    json_data, status = utils.sample_collection(api_key, model, model_id, field_ids, skip == "true")

    return Response(json_data, status=status, mimetype='application/json')

@functools.lru_cache(maxsize=256)
@bp.route("/count/<api_key>/<field_id>", methods=["GET"])
def execute_count(api_key, field_id):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"count": 0}), status=400, mimetype='application/json')

    schemas, secretkey = generate_airtable_schema(api_key)
    airtable = AirTableConnection(decrypt(secretkey), api_key)
    record = airtable.get_record_by_formula("Model", match({"ID": field_id}))

    if record is None:
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    if 'SparQL_Count_Total' not in record['fields']:
        transformer = SparqlTransformer(api_key, field_id)
        transformer.create_model_where()

        select_query = SPARQLSelectQuery()
        select_query.set_where_pattern(transformer.create_model_where())
        query = select_query.get_text()
    else:
        query=record['fields']['SparQL_Count_Total']


    sparql_endpoint = scraper_definition["sparqlendpoint"]
    if "sparql" in sparql_endpoint:
        res = requests.post(sparql_endpoint, data={"query": query}, headers={"Accept": "application/json"})
    else:
        res = requests.post(sparql_endpoint, data=query, headers={"Accept": "application/json", "Content-Type": "application/sparql-query"})

    if not res.ok:
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    json_data = res.json()
    return Response(json.dumps({"count":json_data['results']['bindings'][0]['subject_count']['value']}), status=200, mimetype='application/json')

@functools.lru_cache(maxsize=256)
@bp.route("/sample/<api_key>/<field_id>", methods=["GET"])
def execute_model_sample(api_key, field_id):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"count": 0}), status=400, mimetype='application/json')

    schemas, secretkey = generate_airtable_schema(api_key)
    airtable = AirTableConnection(decrypt(secretkey), api_key)
    record = airtable.get_record_by_formula("Model", match({"ID": field_id}))

    if record is None:
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    if 'SparQL_List_Total' not in record['fields']:
        transformer = SparqlTransformer(api_key, field_id)
        transformer.create_model_where()

        select_query = SPARQLSelectQuery()
        select_query.set_where_pattern(transformer.create_model_where())
        query = select_query.get_text()
    else:
        query=record['fields']['SparQL_List_Total'].replace("LIMIT 100", "LIMIT 5")

    sparql_endpoint = scraper_definition["sparqlendpoint"]
    if "sparql" in sparql_endpoint:
        res = requests.post(sparql_endpoint, data={"query": query}, headers={"Accept": "application/json"})
    else:
        res = requests.post(sparql_endpoint, data=query, headers={"Accept": "application/json", "Content-Type": "application/sparql-query"})

    bindings = res.json()['results']['bindings']
    if not res.ok:
        return Response(json.dumps([]), status=500, mimetype='application/json')

    return Response(json.dumps(list(map(lambda x: x['subject']['value'], bindings))), status=200, mimetype='application/json')

@bp.route("/count/<api_key>/<item>/<scraper>", methods=["GET"])
def execute_count_csv(api_key, item, scraper):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"count": 0}), status=400, mimetype='application/json')

    schemas, secretkey = generate_airtable_schema(api_key)
    schema = schemas[scraper]
    for tablename, fieldlist in schema.items():
        if not isinstance(fieldlist, dict):
            continue
        if "GroupBy" in fieldlist:
            field_table = tablename
            field_table_group_by = fieldlist["GroupBy"]
        else:
            high_table = tablename

    airtable = AirTableConnection(decrypt(secretkey), api_key)

    fields = list(filter(lambda x: 'Field' in x['fields'], airtable.get_multiple_records_by_formula(
        field_table,
        f'SEARCH("{item}",{{{field_table_group_by}}})',
    )))
    model_fields_ids = list(
        map(
            lambda x: x["fields"]["Field"][0]
            if len(x["fields"]["Field"][0]) > 0
            else x["fields"]["Field"],
            fields,
        )
    )

    all_fields =  airtable.get_multiple_records_by_formula(
        "Field",
        OR(
            *list(
                map(
                    lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
                    model_fields_ids,
                )
            )
        ),
    )

    categories = {}
    field_collection = {}
    for field in all_fields:
        field_data = field.get('fields', {})

        if 'Collection_Deployed' in field_data:
            if isinstance(field_data['Collection_Deployed'], str):
                name = field_data['Collection_Deployed']
            else:
                name = field_data['Collection_Deployed'][0]

            if name[:3] == "rec":
                name = airtable.get_record_by_id("Collection", name)['fields']['UI_Name']
        else:
            name = field_data.get('UI_Name', '') + ": Sample"

        if name:
            field_id = field.get('id', '')

            if name not in categories:
                categories[name] = []  # Change to a list of dicts

            categories[name].append(field_id)
        field_collection[field_id] = {"name":name}

    for field in all_fields:
        field_id = field.get('id', '')
        field_collection[field_id]['count'] = json.loads(utils.count_collection(api_key, high_table, item, "_".join(categories[field_collection[field_id]['name']]))[0])["count"]

    final_data = [[field["fields"]["ID"],field["fields"]["UI_Name"], field["fields"]["System_Name"],json.loads(utils.execute_qa(api_key, high_table, item, field["id"])[0])["count"],field_collection[field.get('id', '')]['name'],field_collection[field.get('id', '')]['count']] for field in all_fields]
    final_data.insert(0, ["ID","UI Name", "System Name", "Count", "Collection", "Total Count"])
    csv_file = io.StringIO()
    csv_writer = csv.writer(csv_file)
    csv_writer.writerows(final_data)

    csv_file.seek(0)

    file_wrapper = FileWrapper(csv_file)

    response = Response(
        file_wrapper,
        mimetype='text/csv')
    response.headers["Content-Disposition"] = f"attachment; filename={'output.csv'}"
    response.headers["Content-Type"] = "text/csv"


    return response

@bp.route("/sparqlendpoint/<api_key>", methods=["GET"])
def find_sparqlendpoint(api_key):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"sparqlendpoint": False}), status=200, mimetype='application/json')
    return Response(json.dumps({"sparqlendpoint": True}), status=200, mimetype='application/json')
