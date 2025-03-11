"""
Created on Mar. 9, 2021

@author: Pete Harris
"""

import logging
import re
from typing import Union
from urllib.parse import unquote_plus, urlparse

from pyairtable import Api
from pyairtable.api import Table
from pyairtable.formulas import OR, EQUAL, STR_VALUE

from ZellijData.SingleGroupedItem import SingleGroupedItem

logging.basicConfig(level=logging.DEBUG)


class AirTableConnection(object):
    """
    Note: all this retry and offset looping code has to happen in the other module, where the data can be stored.
    Leaving the notes here for now though.

    Need to be able to do retries, and also to fail after them if need be.

    Currently getting "[503] Service Unavailable: Backend server is at capacity".

    Also possible to get "[429] Too Many Requests" if you hit it more than 5 requests per sec (per base). Must wait 30 seconds after this.
        - Reponse sometimes includes Retry-After: 3600 (for example) to tell you how long to wait.

    If you try to access the AirTables metadata without having that power activated, you get:
        https://api.airtable.com/v0/meta/bases
        404 Not Found
    """

    def __init__(self, bearerToken, dbaseAPI, friendlyname=""):
        """
        Constructor
        """
        self.bearerToken = bearerToken
        self.airTableBaseAPI = dbaseAPI
        self.friendlyname = friendlyname
        self.airtable = Api(self.bearerToken)
        self.headers = {"Authorization": "Bearer " + self.bearerToken}

    def enrich_linked_data(
        self, data_dict, data_key, record, record_key, table, records
    ):
        for model_id in record["fields"][record_key]:
            response = records.get(model_id)
            if data_dict.get(data_key) is None:
                data_dict[data_key] = []
            response["table"] = table
            data_dict[data_key].append(response)

    def getSingleGroupedItem(
        self,
        idsearchterm,
        schema,
        maxrecords=None,
        sort=None,
        prefill_data=None,
        group_sort=None,
    ):
        """
        Schema is a two-entry dictionary containing tablename:{fields}, that identify the paired Group and Base Data; i.e.
             {
                'Model_fields': {
                    'GroupBy': 'Model',
                    'Turtle RDF': 'Model_Fields_Total_Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Field Name',
                    'Identifier': 'Name'
                },
                'Model': {
                    'Turtle RDF': 'Model_Turtle_Prefix',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            }
        The lower-level data can be identified because it contains a GroupBy field, which is mandatory.
        """
        high_table = None
        high_fields = None
        high_remapper = None

        low_remapper = None
        low_fields = None
        low_table = None
        low_group_by = None

        cache = {}

        for tablename, fieldlist in schema.items():
            if not isinstance(fieldlist, dict):
                continue

            if "GroupBy" in fieldlist:
                low_remapper = fieldlist
                low_fields = list(fieldlist.values())
                low_table = tablename
                low_group_by = fieldlist["GroupBy"]
            else:
                high_remapper = fieldlist
                high_fields = list(fieldlist.values())
                high_table = tablename

        table = self.airtable.table(self.airTableBaseAPI, high_table)
        records = table.all(fields=high_fields)
        high_records = []

        if high_table not in cache:
            cache[high_table] = {}
            for r in records:
                if r["fields"]["ID"] == idsearchterm:
                    high_records.append(r)
                cache[high_table][r["id"]] = r

        # parse response here
        if len(high_records) == 0:
            return None

        schema = table.schema()
        searchtext = high_records[0]["fields"]["ID"]
        highout = {"ID": searchtext}
        for mykey, theirkey in high_remapper.items():
            highout[mykey] = (
                high_records[0]["fields"][theirkey]
                if theirkey in high_records[0]["fields"]
                else ""
            )

            if isinstance(highout[mykey], list) and any(["rec" in field for field in highout[mykey]]):
                records = []
                table_id: Union[str, None] = None

                for field in schema.fields:
                    if field.name == theirkey:
                        table_id = field.options.linked_table_id

                if table_id is None:
                    print("Failed to find linked table id")
                    continue

                records.extend(
                    self.get_multiple_records_by_formula(
                        table_id,
                OR(
                            *list(
                                map(
                                    lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
                                    highout[mykey],
                                )
                            )
                        )
                    )
                )

                highout[mykey] = ", ".join(list(filter(lambda x: x, map(lambda x: x.get("fields", {}).get("ID", x.get("fields", {}).get("Name")), records))))

        out = SingleGroupedItem(highout)

        # Now get all the low items grouped under the group record.
        low_airtable = self.airtable.table(self.airTableBaseAPI, low_table)
        low_records = low_airtable.all(
            fields=low_fields,
            formula=f'SEARCH("{searchtext}",{{{low_group_by}}})',
        )

        for rec in low_records:
            remapped = {}
            for mykey, theirkey in low_remapper.items():
                if theirkey not in rec["fields"]:
                    continue

                if (
                    mykey in prefill_data
                    and prefill_data[mykey]["groupable"]
                    and group_sort
                ):
                    table = group_sort["table"]

                    if table not in cache:
                        group_table = self.airtable.table(self.airTableBaseAPI, table)
                        cache[table] = {}
                        for r in group_table.all(fields=[]):
                            cache[table][r["id"]] = r

                    self.enrich_linked_data(
                        remapped, mykey, rec, theirkey, table, cache[table]
                    )
                elif mykey in prefill_data and prefill_data[mykey]["link"]:
                    table = prefill_data[mykey]["link"]

                    if table not in cache:
                        group_table = self.airtable.table(self.airTableBaseAPI, table)
                        cache[table] = {}
                        for r in group_table.all(fields=[]):
                            cache[table][r["id"]] = r

                    self.enrich_linked_data(
                        remapped, mykey, rec, theirkey, table, cache[table]
                    )
                else:
                    remapped[mykey] = rec["fields"][theirkey]
            out.addFields(rec["id"], remapped)

        if any(
            [prefill_data[key]["function"] == "graph_display" for key in prefill_data]
        ):
            # Need to parse the object's data now.
            out.generateTurtle()
            out.generateRDF()

        return out

    def groupFields(self, item, field=None, group_sort=None):
        if field is None:
            item._GroupedFields["default"] = list(item._GroupedData.values())
            item._GroupedFields = item._GroupedFields.items()
            return

        for values in item._GroupedData.values():
            prefix = values.get(field, "default")
            if isinstance(prefix, list) and len(prefix) > 0:
                prefix = prefix[0].get("fields", {}).get("ID", "default")

            if prefix not in item._GroupedFields:
                item._GroupedFields[prefix] = []
            item._GroupedFields[prefix].append(values)

        item._GroupedFields = item._GroupedFields.items()
        try:
            if group_sort:
                item._GroupedFields = sorted(
                    item._GroupedFields,
                    key=lambda x: x[1][0][group_sort["table"]][0]["fields"][
                        group_sort["order"]
                    ],
                )
        except Exception as ex:
            print(ex)

    def getListOfGroups(self, schema, maxrecords=None, sort=None):
        """
        Schema is a two-entry dictionary containing tablename:{fields}, that identify the paired Group and Base Data; i.e.
             {
                'Model_fields': {
                    'KeyField': 'Name',
                    'GroupBy': 'Model',
                    'Turtle RDF': 'Model_Fields_Total_Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Field Name',
                    'Identifier': 'Name'
                },
                'Model': {
                    'KeyField': 'ID',
                    'Turtle RDF': 'Model_Turtle_Prefix',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            }
        The lower-level data can be identified because it contains a GroupBy field, which is mandatory.
        """
        out = []
        low_table = None
        high_table = None
        high_fields = []
        high_remapper = {}

        for tablename, fieldlist in schema.items():
            # of the two item pairs, need to exclude the "low", so we ignore the one with "GroupBy" in it
            if not isinstance(fieldlist, dict):
                continue

            if "GroupBy" in fieldlist:
                low_table = tablename
            else:
                high_remapper = fieldlist
                high_fields = list(fieldlist.values())
                high_table = tablename

        """
        The key field for the high table is what we use for SEARCH().

        There is always a field in the high list that has the same name as the low table, which contains a list
        of references. So if the list is empty, there are no matching low fields in the category. We want that list
        so we can skip the hyperlink.
        """

        if low_table is not None and low_table not in high_fields:
            high_fields.append(low_table)
            high_remapper["Contains"] = low_table

        if low_table in high_fields and "Contains" not in high_fields:
            high_remapper["Contains"] = low_table

        if high_table is None:
            return out

        table = self.airtable.table(self.airTableBaseAPI, high_table)

        records = table.all(
            fields=high_fields,
        )
        schema = table.schema()
        cache = {}
        for rec in records:
            remapped = {}
            for mykey, theirkey in high_remapper.items():
                remapped[mykey] = (
                    rec["fields"][theirkey] if theirkey in rec["fields"] else ""
                )

                if isinstance(remapped[mykey], list) and any(["rec" in field for field in remapped[mykey]]):
                    fetched_records = []
                    table_id: Union[str, None] = None

                    for field in schema.fields:
                        if field.name == theirkey:
                            table_id = field.options.linked_table_id

                    if table_id is None:
                        print("Failed to find linked table id")
                        continue

                    if table_id not in cache:
                        cache[table_id] = self.get_all_records_from_table(table_id)

                    fetched_records.extend(
                        list(filter(lambda x: x['id'] in remapped[mykey], cache[table_id]))
                    )

                    remapped[mykey] = ", ".join(list(filter(lambda x: x, map(lambda x: x.get("fields", {}).get("ID", x.get("fields", {}).get("Name")), fetched_records))))

            out.append(remapped)

        return out

    def getsinglerecord(self, tablename, fieldlist):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        fields = fieldlist.values()

        table = self.airtable.table(self.airTableBaseAPI, tablename)
        record = table.first(fields=fields)

        return record

    def get_record_by_formula(self, table_name: str, formula: str):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        table: Table = self.airtable.table(self.airTableBaseAPI, table_name)
        return table.first(formula=formula)

    def get_multiple_records_by_formula(self, table, formula):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        table = self.airtable.table(self.airTableBaseAPI, table)
        return table.all(formula=formula)

    def get_all_records_from_table(self, table):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        table = self.airtable.table(self.airTableBaseAPI, table)
        return table.all()

    def get_record_by_id(self, table, record_id):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        table = self.airtable.table(self.airTableBaseAPI, table)
        return table.get(record_id)

    def _fixarrows(self, txt):
        """Convert text arrows to Unicode arrows"""
        return re.sub(r"\-\>", "→", txt)


class EnhancedResponse(object):
    def __init__(self, url, response, dbasename="", apikey="", custom={}):
        self.url = url
        self.response = response
        self.dbasename = dbasename
        self.apikey = apikey
        self.custom = custom
        self.status_code = response.status_code
        self.message = ""
        err = response.json()["error"]
        if isinstance(err, dict):
            self.type = err["type"] if "type" in err else str(err)
            self.message = err["message"] if "message" in err else ""
        else:
            self.type = err

        self.urlparse = urlparse(url)
        chunks = self.urlparse.path.split(self.apikey)
        self.urlpreapi = chunks[0]
        self.urltable = chunks[1][1:]  # leading slash

        self.urlparams = {}
        for p in self.urlparse.query.split("&"):
            k, v = p.split("=")
            if unquote_plus(k) not in self.urlparams:
                self.urlparams[unquote_plus(k)] = []
            self.urlparams[unquote_plus(k)].append(unquote_plus(v))

    def __str__(self):
        txt = "<EnhancedResponse [" + str(self.status_code) + "]>"
        if self.message:
            txt += " " + self.message
        txt += "\n"
        if "http" in self.urlparse.scheme:
            txt += self.urlparse.scheme + "://"
        txt += self.urlparse.path + self.urlparse.params
        if self.urlparse.query:
            txt += "..." + str(self.urlparams.keys())
        return txt


class AirTableError(Exception):
    def __init__(self, url, response):
        self.url = url
        self.response = response
