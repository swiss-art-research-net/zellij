"""
Created on Mar. 18, 2021
@author: Pete Harris
"""
import asyncio
import io
import logging

from flask import Blueprint, render_template, request, abort, send_file

from ZellijData.AirTableConnection import AirTableConnection, EnhancedResponse
from website.datasources import get_prefill
from website.db import get_db, dict_gen_many, generate_airtable_schema, decrypt
from website.exporters.ModelExporter import ModelExporter
from website.functions import functions

bp = Blueprint("docs", __name__, url_prefix="/docs")


@bp.route("/", methods=["GET"])
def main():
    db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT *, COUNT(dbasekey) AS Count FROM AirTableDatabases"
        + " LEFT JOIN AirTableAccounts"
        + " ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers"
        + " ON dbaseid = dbasekey"
        + " WHERE scraperid IS NOT NULL"
        + " GROUP BY dbasekey"
    )
    dblist = [d for d in dict_gen_many(c)]
    c.close()
    return render_template("docs/databaselist.html", databases=dblist, fields={})


@bp.route("/searchBaseList", methods=["GET", "POST"])
async def search_baselist():
    search_query = request.args.get("search_query")

    if search_query is None:
        return render_template(
            "docs/databaselist.html",
            databases=[],
            fields=[],
            search_query=search_query,
        )

    db = get_db()
    c = db.cursor()

    query = """
        SELECT atst.dbasename , atat.accountname
        FROM AirTableDatabases AS atst
        JOIN AirTableAccounts AS atat
        ON atat.accountid = atst.airtableaccountkey
        WHERE atst.dbasename LIKE %s
            OR  atat.accountname LIKE %s
    """
    c.execute(query, (search_query, search_query))

    results = [d for d in dict_gen_many(c)]

    db_query = """
        SELECT atst.dbasename , atst.dbaseapikey
        FROM AirTableDatabases AS atst
    """
    c.execute(db_query)

    tasks = []
    fields = {}
    for db_result in dict_gen_many(c):
        api_key = db_result["dbaseapikey"]
        db_name = db_result["dbasename"]

        schemas, secretkey = generate_airtable_schema(api_key)
        tasks.append(
            asyncio.create_task(
                searchAirtable(
                    api_key, db_name, fields, schemas, search_query, secretkey
                )
            )
        )

    await asyncio.gather(*tasks)

    return render_template(
        "docs/databaselist.html",
        databases=results,
        fields=fields,
        search_query=search_query,
    )


async def searchAirtable(api_key, db_name, fields, schemas, search_query, secretkey):
    airtable = AirTableConnection(decrypt(secretkey), api_key)
    for schema in schemas:
        airtable_results = airtable.getListOfGroups(schemas[schema])
        if schema not in fields:
            fields[schema] = []

        if isinstance(airtable_results, EnhancedResponse):
            continue

        for result in airtable_results:
            if len(result.get("Name", "")) == 0:
                continue

            if isinstance(result.get("Name", ""), list):
                continue

            if search_query.lower() not in result.get("Name", "").lower():
                continue

            if len(result["Contains"]) == 0:
                continue

            data = {
                "type": schema,
                "apikey": api_key,
                "name": result["Name"],
                "id": result["KeyField"],
                "db": db_name,
            }
            fields[schema].append(data)


@bp.route("/MultIndex", methods=["GET", "POST"])
def multipleIndexGeneration():
    db = get_db()
    c = db.cursor()

    # ---field-------------------------------------------------------------

    c.execute(
        """
             SELECT  atdb.dbasename ,atdb.dbaseapikey , airt.accountname ,scrapername
              FROM Scrapers as scrap
              JOIN AirTableDatabases AS atdb
              JOIN AirTableAccounts AS airt
              ON atdb.dbaseid = scrap.dbasekey AND atdb.airtableaccountkey = airt.accountid
              WHERE `scrapername` LIKE '%field%'
            """
    )

    FieldIndex = {}
    field_keys = []

    field_data_index = {}

    for dt in dict_gen_many(c):
        apikey = dt["dbaseapikey"]
        dbname = dt["dbasename"]
        account_name = dt["accountname"]

        new_dict = {
            apikey: [{"dbname": [], "account_name": []}],
        }
        field_keys.append(apikey)

        new_dict[apikey][0]["dbname"].append(dbname)
        new_dict[apikey][0]["account_name"].append(account_name)

        FieldIndex[apikey] = ([{"dbname": [dbname], "account_name": [account_name]}],)

        schemas, secretkey = generate_airtable_schema(apikey)
        airtable = AirTableConnection(decrypt(secretkey), apikey)
        for schema in schemas:
            results = airtable.getListOfGroups(schemas[schema])

            if isinstance(results, EnhancedResponse):
                logging.error(results)
                continue

            for result in results:
                if len(result.get("Name", "")) == 0:
                    continue

                if len(result["Contains"]) == 0:
                    continue

                first_letter = result["Name"][0].lower()
                if first_letter not in field_data_index:
                    field_data_index[first_letter] = []

                data = {
                    "type": schema,
                    "apikey": apikey,
                    "name": result["Name"],
                    "id": result["KeyField"],
                    "db": dbname,
                    "authority": account_name,
                }
                field_data_index[first_letter].append(data)

    c.close()
    return render_template(
        "multipleIndex/multipleIndex.html",
        field=FieldIndex,
        fkeys=field_keys,
        fields=field_data_index,
    )


@bp.route("/list/<apikey>", methods=["GET"])
def patternlistall(apikey):
    return _patternlister(apikey)


@bp.route("/list/<apikey>/export/<exportType>/<model>", methods=["GET"])
def patternlistexport(apikey, exportType, model):
    exporters = {
        'model': ModelExporter,
        'collection': ModelExporter
    }

    item = request.args.get("item")
    exporter = exporters[exportType]()
    exporter.initialize(model, apikey, item)

    file = exporter.export()

    return send_file(file, as_attachment=True, download_name="test.xml", mimetype="text/xml")



@bp.route("/list/<apikey>/<pattern>", methods=["GET"])
def patternlist(apikey, pattern):
    return _patternlister(apikey, pattern=pattern)


def _patternlister(apikey, pattern=None):
    scraper = request.args.get("scraper")
    schemas, secretkey = generate_airtable_schema(apikey)
    airtable = AirTableConnection(decrypt(secretkey), apikey)
    lists = {}
    if pattern in schemas:
        result = airtable.getListOfGroups(schemas[pattern])
        if isinstance(result, EnhancedResponse):
            return render_template("error/airtableerror.html", error=result)
        lists[pattern] = result
    else:
        for key, val in schemas.items():
            result = airtable.getListOfGroups(val)
            if isinstance(result, EnhancedResponse):
                return render_template("error/airtableerror.html", error=result)
            lists[key] = result

    if not scraper and len(lists) > 0:
        scraper = list(lists.keys())[0]
    scraper_id = schemas[scraper]["id"]

    _, prefill_group, _ = get_prefill(apikey, scraper_id)

    return render_template(
        "docs/showgroups.html",
        lists=lists,
        apikey=apikey,
        scraper=scraper,
        prefill_group=prefill_group,
    )


@bp.route("/display/<apikey>/<pattern>", methods=["GET"])
def patternitemdisplay(apikey, pattern):
    groupref = request.args.get("search")
    schemas, secretkey = generate_airtable_schema(apikey)
    airtable = AirTableConnection(decrypt(secretkey), apikey)

    if pattern not in schemas:
        abort(404)

    schema = schemas[pattern]
    prefill_data, prefill_group, group_sort = get_prefill(apikey, schema.get("id"))
    item = airtable.getSingleGroupedItem(
        groupref, schema, prefill_data=prefill_data, group_sort=group_sort
    )

    if (
        isinstance(item, EnhancedResponse)
        or item is None
        or isinstance(prefill_data, str)
    ):
        return render_template("error/airtableerror_simple.html", error=item)

    fields_to_group = [
        key for key, value in prefill_data.items() if value.get("groupable", False)
    ]
    if len(fields_to_group) > 0:
        airtable.groupFields(item, fields_to_group[0], group_sort)
    else:
        airtable.groupFields(item)

    if not isinstance(prefill_data, str):
        for key, value in prefill_data.items():
            if value.get("sortable", False):
                items = item._GroupedData
                if all(
                    [
                        isinstance(x.get(key, False), str) and x.get(key, "").isdigit()
                        for x in items.values()
                    ]
                ):
                    for x in items.values():
                        x[key] = int(x[key])

    return render_template(
        "docs/showitem.html",
        apikey=apikey,
        item=item,
        pattern=pattern,
        prefill=prefill_data,
        prefill_group=prefill_group,
        functions=functions,
        group_sort=group_sort,
    )
