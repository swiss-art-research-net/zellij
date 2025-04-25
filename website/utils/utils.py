import functools
import json

import requests

from SPARQLBurger.SPARQLQueryBuilder import SPARQLGraphPattern, SPARQLSelectQuery
from website.db import get_scraper_definition
from website.transformers.SparqlTransformer import SparqlTransformer


@functools.lru_cache(maxsize=256)
def execute_qa(api_key, model, model_id, field_id):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps({"count": 0}), 400]

    transformer = SparqlTransformer(api_key, field_id)
    transformer.transform(count=True, model=model, model_id=model_id)

    sparql_endpoint = scraper_definition["sparqlendpoint"]
    if "sparql" in sparql_endpoint:
        res = requests.post(
            sparql_endpoint,
            data={"query": transformer.sparql},
            headers={"Accept": "application/json"},
        )
    else:
        res = requests.post(
            sparql_endpoint,
            data=transformer.sparql,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/sparql-query",
            },
        )

    if not res.ok:
        return [json.dumps({"count": 0}), 500]

    json_data = res.json()

    return [
        json.dumps({"count": json_data["results"]["bindings"][0]["count"]["value"]}),
        200,
    ]


@functools.lru_cache(maxsize=512)
def count_collection(api_key, model, model_id, ids):
    ids = ids.split("_")
    query = SPARQLSelectQuery(limit=1)
    query.add_variables(["(COUNT(DISTINCT ?value) as ?count)"])
    where_pattern = SPARQLGraphPattern()
    for i, id in enumerate(ids):
        transformer_loop = SparqlTransformer(api_key, id)

        if i == 0:
            transformer_loop.add_prefixes(query)

        where = transformer_loop.create_where_pattern(
            model=model, model_id=model_id, union=i > 0
        )
        where_pattern.add_nested_graph_pattern(where)
    query.set_where_pattern(where_pattern)
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps({"count": 0}), 400]
    text_query = query.get_text()
    sparql_endpoint = scraper_definition["sparqlendpoint"]
    if "sparql" in sparql_endpoint:
        res = requests.post(
            sparql_endpoint,
            data={"query": text_query},
            headers={"Accept": "application/json"},
        )
    else:
        res = requests.post(
            sparql_endpoint,
            data=text_query,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/sparql-query",
            },
        )

    if not res.ok:
        return [json.dumps({"count": 0}), 500]

    json_data = res.json()

    return [
        json.dumps({"count": json_data["results"]["bindings"][0]["count"]["value"]}),
        200,
    ]


@functools.lru_cache(maxsize=512)
def sample_collection(api_key, model, model_id, ids, skip=False):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps([]), 400]

    sparql_endpoint = scraper_definition["sparqlendpoint"]

    ids = ids.split("_")

    samples = {}
    for i, id in enumerate(ids):

        query = SPARQLSelectQuery(distinct=True, limit=5)
        transformer_loop = SparqlTransformer(api_key, id)
        transformer_loop.add_prefixes(query)

        if skip:
            samples[transformer_loop.get_field_or_default("UI_Name")] = {"id": transformer_loop.field.get("id"), "samples": []}
            continue

        query.add_variables(["?" + transformer_loop.self_uri])
        where = transformer_loop.create_where_pattern(
            model=model, model_id=model_id, union=False
        )
        query.set_where_pattern(where)

        text_query = query.get_text()

        if "sparql" in sparql_endpoint:
            res = requests.post(sparql_endpoint, data={"query": text_query}, headers={"Accept": "application/json"})
        else:
            res = requests.post(sparql_endpoint, data=text_query, headers={"Accept": "application/json", "Content-Type": "application/sparql-query"})

        if not res.ok:
            samples[transformer_loop.get_field_or_default("UI_Name")] = {"id": transformer_loop.field.get("id"), "samples": []}
            continue

        bindings = res.json()['results']['bindings']
        if len(bindings) == 0:
            samples[transformer_loop.get_field_or_default("UI_Name")] = {"id": transformer_loop.field.get("id"), "samples": []}
            continue

        samples[transformer_loop.get_field_or_default("UI_Name")] = {"id": transformer_loop.field.get("id"), "samples": list(map(lambda x: x[transformer_loop.self_uri]['value'], bindings))}

    return [json.dumps(samples), 200]
