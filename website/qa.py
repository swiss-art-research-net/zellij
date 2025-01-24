from flask import Blueprint, Response
from website.transformers.SparqlTransformer import SparqlTransformer
import json
from website.db import get_scraper_definition, decrypt, generate_airtable_schema
import requests
from ZellijData.AirTableConnection import AirTableConnection
from pyairtable.formulas import match


bp = Blueprint('qa', __name__, url_prefix='/qa')

@bp.route("/<api_key>/<field_id>", methods=["GET"])
def execute_qa(api_key, field_id):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return Response(json.dumps({"count": 0}), status=400, mimetype='application/json')

    transformer = SparqlTransformer(api_key, field_id)
    transformer.transform(count=True)

    res = requests.post(scraper_definition["sparqlendpoint"], data={"query": transformer.sparql}, headers={"Accept": "application/json"})

    if not res.ok:
        print(res.text, transformer.sparql)
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    json_data = res.json()

    return Response(json.dumps({"count": json_data['results']['bindings'][0]['count']['value']}), status=200, mimetype='application/json')

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
    

    print("here is the airtable", record['fields']['SparQL_Count_Total'])
    query=record['fields']['SparQL_Count_Total']

    res = requests.post(scraper_definition["sparqlendpoint"], data={"query": query}, headers={"Accept": "application/json"})

    if not res.ok:
        print(res.text, query)
        return Response(json.dumps({"count": 0}), status=500, mimetype='application/json')

    json_data = res.json()
    return Response(json.dumps({"count":json_data['results']['bindings'][0]['subject_count']['value']}), status=200, mimetype='application/json')
