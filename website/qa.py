from flask import Blueprint, Response
from website.transformers.SparqlTransformer import SparqlTransformer
import json
from website.db import get_scraper_definition
import requests


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
