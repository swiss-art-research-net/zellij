"""
Created on Mar. 18, 2021
@author: Pete Harris
"""

import asyncio
import logging
import zipfile
from abc import ABCMeta
from io import BytesIO
from typing import Dict

from flask import Blueprint, Response, abort, g, render_template, request
from werkzeug.wsgi import FileWrapper

from website.auth import login_required
from website.datasources import get_prefill
from website.db import (
    decrypt,
    dict_gen_many,
    dict_gen_one,
    generate_airtable_schema,
    get_base_name,
    get_db,
)
from website.exporters.FieldExporter import FieldExporter
from website.exporters.ModelExporter import ModelExporter
from website.exporters.ProjectExporter import ProjectExporter
from website.functions import functions
from website.github_wrapper import GithubWrapper
from website.transformers.ResearchSpaceTransformer import ResearchSpaceTransformer
from website.transformers.SparqlTransformer import SparqlTransformer
from website.transformers.TurtleTransformer import TurtleTransformer
from website.transformers.X3MLTransformer import X3MLTransformer
from ZellijData.AirTableConnection import AirTableConnection, EnhancedResponse
from pyairtable.formulas import EQUAL, OR, STR_VALUE

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
    grouped_dbs = {}

    for db in dblist:
        if db["accountname"] not in grouped_dbs:
            grouped_dbs[db["accountname"]] = []
        grouped_dbs[db["accountname"]].append(db)

    return render_template("docs/databaselist.html", databases=grouped_dbs, fields={})


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
        "model": ModelExporter,
        "collection": ModelExporter,
        "field": FieldExporter,
        "project": ProjectExporter,
    }

    item = request.args.get("item")
    exporter = exporters[exportType]()
    exporter.initialize(model, apikey, item)

    file = exporter.export()

    if exporter.get_name():
        filename = exporter.get_name()
    elif item is None:
        filename = model
    else:
        filename = "".join(
            c if c.isalpha() or c.isdigit() or c == " " else "_" for c in item
        ).rstrip()

    w = FileWrapper(file)

    response = Response(w, mimetype="text/xml", direct_passthrough=True)
    response.headers["Content-Disposition"] = f"attachment; filename={filename}.xml"
    response.headers["Content-Type"] = "application/xml"
    return response


@bp.route("/list/<apikey>/export/<exportType>/<model>/all", methods=["GET"])
@login_required
def patternlistexporttree(apikey, exportType, model):
    exporters: Dict[str, ABCMeta] = {
        "model": ModelExporter,
        "collection": ModelExporter,
        "field": FieldExporter,
        "project": ProjectExporter,
    }

    database = get_db()
    c = database.cursor()
    c.execute("SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (apikey,))
    existing = dict_gen_one(c)

    github = None

    if existing is not None and "githubtoken" in existing:
        existing["githubtoken"] = decrypt(existing["githubtoken"])
        github = GithubWrapper(
            existing["githubtoken"],
            existing["githubrepo"],
            existing["githuborganization"],
        )

    item = request.args.get("item")

    files = []
    if exportType == "project":
        exporter = exporters[exportType]().initialize(model, apikey, item)
        file = exporter.export()
        files.append({"name": exporter.get_name(), "file": file})
        if github:
            github.upload_file(f"space/{exporter.get_name()}.xml", file)

        schemas, secretkey = generate_airtable_schema(apikey)
        airtable = AirTableConnection(decrypt(secretkey), apikey)
        lists = {}
        for key, val in schemas.items():
            result = airtable.getListOfGroups(val)
            if isinstance(result, EnhancedResponse):
                return render_template("error/airtableerror.html", error=result)
            lists[key] = result

        for key, val in lists.items():
            for item in val:
                exporter = exporters["model"]().initialize(
                    key, apikey, item["KeyField"]
                )
                file = exporter.export()
                files.append({"name": f"{key}_{exporter.get_name()}", "file": file})
                if github:
                    github.upload_file(f"composite/{exporter.get_name()}.xml", file)

                for field in item["Contains"]:
                    field_exporter = exporters["field"]().initialize(key, apikey, field)
                    file = field_exporter.export()
                    files.append(
                        {
                            "name": f"{key}_{exporter.get_name()}_{field_exporter.get_name()}",
                            "file": file,
                        }
                    )
                    if github:
                        github.upload_file(
                            f"atom/{exporter.get_name()}_{field_exporter.get_name()}.xml",
                            file,
                        )
    if exportType == "model" or exportType == "collection":
        schemas, secretkey = generate_airtable_schema(apikey)
        airtable = AirTableConnection(decrypt(secretkey), apikey)
        exporter = exporters["model"]().initialize(model, apikey, item)
        file = exporter.export()
        files.append({"name": f"{exporter.get_name()}", "file": file})
        if github:
            github.upload_file(f"space/{exporter.get_name()}.xml", file)

        prefill_data, prefill_group, group_sort = get_prefill(
            apikey, exporter.get_schema().get("id")
        )
        fields = airtable.getSingleGroupedItem(
            item,
            exporter.get_schema(),
            prefill_data=prefill_data,
            group_sort=group_sort,
        )

        for idx, field in enumerate(fields._GroupedData.values()):
            print(f"Processing {idx} of {len(fields._GroupedData)}")
            field_exporter = exporters["field"]().initialize(
                model, apikey, field["KeyField"]
            )
            file = field_exporter.export()
            files.append(
                {
                    "name": f"{exporter.get_name()}_{field_exporter.get_name()}",
                    "file": file,
                }
            )
            if github:
                github.upload_file(
                    f"atom/{exporter.get_name()}_{field_exporter.get_name()}.xml", file
                )

    zip_stream = BytesIO()
    with zipfile.ZipFile(zip_stream, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            zf.writestr(f"{file['name']}.xml", file["file"].read())

    zip_stream.seek(0)
    w = FileWrapper(zip_stream)

    response = Response(w, mimetype="application/zip", direct_passthrough=True)
    response.headers["Content-Disposition"] = "attachment; filename=export.zip"
    response.headers["Content-Type"] = "application/zip"
    return response


@bp.route("/transform/turtle/<apikey>/<item>")
def patterntransformturtle(apikey, item):
    transformer = TurtleTransformer(apikey, item)
    file = transformer.transform()

    if request.args.get("upload") == "true":
        if g.user is None:
            return "", 401

        try:
            transformer.upload()

            return "", 200
        except:
            return "", 500
    else:
        w = FileWrapper(file)

        response = Response(w, mimetype="text/turtle", direct_passthrough=True)
        response.headers["Content-Disposition"] = f"attachment; filename={file.name}"
        response.headers["Content-Type"] = "text/turtle"
        return response


@bp.route("/transform/sparql/<apikey>/<item>")
def patterntransformsparql(apikey, item):
    transformer = SparqlTransformer(apikey, item)
    file = transformer.transform()

    if request.args.get("upload") == "true":
        if g.user is None:
            return "", 401

        try:
            transformer.upload()

            return "", 200
        except:
            return "", 500
    else:
        w = FileWrapper(file)

        response = Response(w, mimetype="text/plain", direct_passthrough=True)
        response.headers["Content-Disposition"] = f"attachment; filename={file.name}"
        response.headers["Content-Type"] = "text/plain"
        return response


@bp.route("/transform/x3ml/<apikey>/<pattern>/<modelid>/<item>/<formtype>")
def patterntransformx3ml(apikey, pattern, modelid, item, formtype):
    transformer = X3MLTransformer(
        apikey, pattern, modelid, item if item != "model" else None
    )
    file = transformer.transform(form=formtype)

    if request.args.get("upload") == "true":
        if g.user is None:
            return "", 401

        try:
            transformer.upload(formtype)

            return "", 200
        except:
            return "", 500
    else:
        w = FileWrapper(file)

        response = Response(w, mimetype="application/xml", direct_passthrough=True)
        response.headers["Content-Disposition"] = f"attachment; filename={file.name}"
        response.headers["Content-Type"] = "application/xml"
        return response


@bp.route("/transform/rs/<apikey>/<pattern>/<modelid>/<item>")
def patterntransformrs(apikey, pattern, modelid, item):
    transformer = ResearchSpaceTransformer(
        apikey, pattern, modelid, item if item != "model" else None
    )
    file = transformer.transform()

    if request.args.get("upload") == "true":
        if g.user is None:
            return "", 401

        try:
            transformer.upload()

            return "", 200
        except:
            return "", 500
    else:
        w = FileWrapper(file)

        response = Response(w, mimetype="text/yaml", direct_passthrough=True)
        response.headers["Content-Disposition"] = f"attachment; filename={file.name}"
        response.headers["Content-Type"] = "text/yaml"
        return response


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
        name=get_base_name(apikey),  # noqa: F821
    )


@bp.route("/display/<apikey>/<pattern>", methods=["GET"])
def patternitemdisplay(apikey, pattern):
    groupref = request.args.get("search")
    schemas, secretkey = generate_airtable_schema(apikey)
    airtable = AirTableConnection(decrypt(secretkey), apikey)

    if pattern not in schemas:
        abort(404)

    schema: dict = schemas[pattern]
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

    model_id = item.ID
    model_table = None
    for key, value in schema.items():
        if not isinstance(value, dict):
            continue

        if "GroupBy" not in value:
            model_table = key
            break

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

    #### Fetch Collection Categories ####

    categories = {}
    prefix_categories = {}
    for obj in item.GroupedFields():
        identifiers = []
        for el in obj[1]:
            if 'Field' not in el:
                continue
            identifiers.append(el['Field'][0])

        fields = airtable.get_multiple_records_by_formula("Field",
            OR(
                *list(
                    map(
                        lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
                        identifiers,
                    )
                )
            ),
        )
        prefix_cat = {}

        for field in fields:
            field_data = field.get('fields', {})

            if 'Collection_Deployed' in field_data:
                if isinstance(field_data['Collection_Deployed'], str):
                    name = field_data['Collection_Deployed']
                else:
                    name = field_data['Collection_Deployed'][0]

                if name[:3] == "rec":
                    name = airtable.get_record_by_id("Collection", name)['fields']['UI_Name'] + ": Sample"
            else:
                name = field_data.get('UI_Name', '') + ": Sample"

            if not name:
                continue

            field_id = field.get('id', '')
            field_ui_name = field_data.get('UI_Name', '')  # Get UI name or default to ""

            if name not in categories:
                categories[name] = []  # Change to a list of dicts

            if name not in prefix_cat:
                prefix_cat[name] = []

            categories[name].append({"id": field_id, "ui_name": field_ui_name})
            prefix_cat[name].append({"id": field_id, "ui_name": field_ui_name})

        if obj[0] not in prefix_categories:
            prefix_categories[obj[0]] = prefix_cat
        else:
            prefix_categories[obj[0]].update(prefix_cat)

    return render_template(
        "docs/showitem.html",
        apikey=apikey,
        item=item,
        pattern=pattern,
        prefill=prefill_data,
        prefill_group=prefill_group,
        functions=functions,
        group_sort=group_sort,
        airtable = airtable,
        categories=[categories, prefix_categories],
        model_id=model_id,
        model_table=model_table,
    )
