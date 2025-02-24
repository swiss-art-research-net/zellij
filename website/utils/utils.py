from flask import  Response
from website.transformers.SparqlTransformer import SparqlTransformer
import json
from website.db import get_scraper_definition
import requests
import functools
from SPARQLBurger.SPARQLQueryBuilder import (SPARQLSelectQuery, SPARQLGraphPattern)


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

@functools.lru_cache(maxsize=512)
def count_collection(api_key,ids):
    ids = ids.split("_")
    query = SPARQLSelectQuery()
    query.add_variables(["(COUNT(?value) as ?count)"])
    transformer = SparqlTransformer(api_key, ids[0]) #use first id to get prefixes
    transformer.add_prefixes(query)
    where_pattern = SPARQLGraphPattern()
    for id in ids:
        transformer_loop = SparqlTransformer(api_key, id)
        where = transformer_loop.create_where_pattern(optional=True)
        where_pattern.add_nested_graph_pattern(where)
    query.set_where_pattern(where_pattern)
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps({"count": 0}), 400]
    text_query = query.get_text()
    res = requests.post(scraper_definition["sparqlendpoint"], data={"query": text_query}, headers={"Accept": "application/json"})

    if not res.ok:
        print(res.text, text_query)
        return [json.dumps({"count": 0}), 500]

    json_data = res.json()

    return [json.dumps({"count": json_data['results']['bindings'][0]['count']['value']}), 200]