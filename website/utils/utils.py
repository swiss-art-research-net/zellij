from flask import  Response
from website.transformers.SparqlTransformer import SparqlTransformer
import json
from website.db import get_scraper_definition
import requests
import functools


@functools.lru_cache(maxsize=256)
def execute_qa(api_key, field_id):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps({"count": 0}), 400]

    transformer = SparqlTransformer(api_key, field_id)
    transformer.transform(count=True)

    res = requests.post(scraper_definition["sparqlendpoint"], data={"query": transformer.sparql}, headers={"Accept": "application/json"})

    if not res.ok:
        print(res.text, transformer.sparql)
        return [json.dumps({"count": 0}), 500]

    json_data = res.json()

    return [json.dumps({"count": json_data['results']['bindings'][0]['count']['value']}), 200]