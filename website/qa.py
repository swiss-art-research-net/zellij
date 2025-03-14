from flask import Blueprint, Response
from website.transformers.SparqlTransformer import SparqlTransformer
import json
from website.db import get_scraper_definition, decrypt, generate_airtable_schema
import requests
from ZellijData.AirTableConnection import AirTableConnection
from pyairtable.formulas import match
import functools
import website.utils.utils as utils
from pyairtable.formulas import EQUAL, OR, STR_VALUE
from werkzeug.wsgi import FileWrapper
import csv
import io


bp = Blueprint('qa', __name__, url_prefix='/qa')

@bp.route("/<api_key>/<field_id>", methods=["GET"])
def return_execute_qa(api_key, field_id):
    json_data, status = utils.execute_qa(api_key, field_id)

    return Response(json_data, status=status, mimetype='application/json')

@bp.route("/collection/count/<api_key>/<field_ids>", methods=["GET"])
def return_collection_count(api_key, field_ids):
    json_data, status = utils.count_collection(api_key, field_ids)

    return Response(json_data, status=status, mimetype='application/json')

@bp.route("/collection/sample/<api_key>/<field_ids>", methods=["GET"])
def return_collection_sample(api_key, field_ids):
    json_data, status = utils.sample_collection(api_key, field_ids)

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
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')
    
    query=record['fields']['SparQL_Count_Total']

    res = requests.post(scraper_definition["sparqlendpoint"], data={"query": query}, headers={"Accept": "application/json"})

    if not res.ok:
        print(res.text, query)
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    json_data = res.json()
    return Response(json.dumps({"count":json_data['results']['bindings'][0]['subject_count']['value']}), status=200, mimetype='application/json')

@bp.route("/count/<api_key>/<item>/<table>", methods=["GET"])
def execute_count_csv(api_key, item, table):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"count": 0}), status=400, mimetype='application/json')
    schemas, secretkey = generate_airtable_schema(api_key)
    schema = schemas[table]
    for tablename, fieldlist in schema.items():
        if not isinstance(fieldlist, dict):
            continue
        if "GroupBy" in fieldlist:
            field_table = tablename
            field_table_group_by = fieldlist["GroupBy"]
        else:
            pattern = tablename
    airtable = AirTableConnection(decrypt(secretkey), api_key)
    records = []
    if isinstance(item, str):
        if "," in item:
            items = item.split(", ")

            for record in items:
                records.append(
                    airtable.get_record_by_formula(
                        table, match({"ID": record})
                    )
                )
        elif "rec" in item:
            records.append(airtable.get_record_by_id(table, item))
        else:
            records.append(
                airtable.get_record_by_formula(table, match({"ID": item}))
            )
    else:
        for record in item:
            records.append(airtable.get_record_by_id(table, record))

    model_fields_ids = list(
        map(
            lambda x: x["fields"]["Field"][0]
            if len(x["fields"]["Field"][0]) > 0
            else x["fields"]["Field"],
            airtable.get_multiple_records_by_formula(
                field_table,
                f'SEARCH("{item}",{{{field_table_group_by}}})',
            ),
        )
    )
    all_fields = list(
                filter(
                    lambda field: len(
                        field.get("fields", {}).get("Collection_Deployed", "")
                    )
                    > 0,
                    airtable.get_multiple_records_by_formula(
                        "Field",
                        OR(
                            *list(
                                map(
                                    lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
                                    model_fields_ids,
                                )
                            )
                        ),
                    ),
                )
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
            name = field_data.get('UI_Name') + ":Sample"

        if name:
            field_id = field.get('id', '')
            
            if name not in categories:
                categories[name] = []  # Change to a list of dicts
            
            categories[name].append(field_id)
        field_collection[field_id] = {"name":name}

    for field in all_fields:
        field_id = field.get('id', '')
        field_collection[field_id]['count'] = json.loads(utils.count_collection(api_key, "_".join(categories[field_collection[field_id]['name']]))[0])["count"]

    final_data = [[field["fields"]["ID"],field["fields"]["UI_Name"], field["fields"]["System_Name"],json.loads(utils.execute_qa(api_key, field["id"])[0])["count"],field_collection[field.get('id', '')]['name'],field_collection[field.get('id', '')]['count']] for field in all_fields]
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
        return Response(json.dumps({"sparqlendpoint": False}), status=400, mimetype='application/json')
    return Response(json.dumps({"sparqlendpoint": True}), status=200, mimetype='application/json')
    
