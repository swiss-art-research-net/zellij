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

    transformer = SparqlTransformer(api_key, field_id, model=model, model_id=model_id)
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
def sample_collection(api_key, model, model_id, ids):
    scraper_definition = get_scraper_definition(api_key)

    if scraper_definition is None or not scraper_definition["sparqlendpoint"]:
        return [json.dumps([]), 400]

    sparql_endpoint = scraper_definition["sparqlendpoint"]

    ids = ids.split("_")
    query = SPARQLSelectQuery(distinct=True, limit=5)
    main_where = SPARQLGraphPattern()
    query.set_where_pattern(main_where)

    common_parts = []
    transformers: dict[str, SparqlTransformer] = {}
    for i, id in enumerate(ids):
        transformer = SparqlTransformer(api_key, id, model=model, model_id=model_id)
        transformers[id] = transformer

        if len(common_parts) == 0:
            common_parts = transformer.parts[:2]

        if transformer.parts[:2] != common_parts:
            raise ValueError(
                f"IDs {id} and {ids[0]} do not have the same parts: {transformer.parts[:2]} vs {common_parts}"
            )

        if i == 0:
            transformer.add_prefixes(query)
            temp = transformer.parts[:]
            transformer.parts = common_parts

            where = transformer.create_where_pattern(
                model=model,
                model_id=model_id,
            )
            transformer.parts = temp[:]
            main_where.add_nested_graph_pattern(where)

    samples = {}
    for i, id in enumerate(ids):
        transformer = transformers[id]

        if transformer.get_field_or_default("UI_Name") not in samples:
            samples[transformer.get_field_or_default("UI_Name")] = {
                "samples": [],
                "labels": [],
                "id": transformer.id,
            }

        query.add_variables(
            ["?" + transformer.self_uri, "?" + transformer.self_uri + "_label"]
        )
        where = transformer.create_where_pattern(optional=True, start=2)

        main_where.add_nested_graph_pattern(where)

    text_query = query.get_text()
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
        return [json.dumps(samples), 500]

    bindings = res.json()["results"]["bindings"]
    for binding in bindings:
        for transformer in transformers.values():
            samples[transformer.get_field_or_default("UI_Name")]["samples"].append(
                binding.get(transformer.self_uri, {}).get("value", "-")
            )
            samples[transformer.get_field_or_default("UI_Name")]["labels"].append(
                binding.get(transformer.self_uri + "_label", {}).get("value", "-")
            )

    return [json.dumps(samples), 200]
