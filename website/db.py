"""
Created on Mar. 11, 2021

@author: Pete Harris
"""

import os
from collections import OrderedDict
from typing import Tuple

import click

# import sqlite3
import MySQLdb  # from pip install mysqlclient
import nacl.secret
from flask import current_app, g
from flask.cli import with_appcontext

from website.DataScraper import DataScraper


def get_db():
    if "db" not in g:
        g.db = MySQLdb.connect(
            user=os.getenv("MYSQL_USER"),
            passwd=os.getenv("MYSQL_PASSWORD"),
            db=os.getenv("MYSQL_DB"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            host=os.getenv("MYSQL_HOST"),
        )
    return g.db


def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()

    with current_app.open_resource("schema.sql") as f:
        db.executescript(f.read().decode("utf-8"))


@click.command("init-db")
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo("Initialized the database.")


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)

    """ Original dict_gen from Python Essential Reference by David Beazley """


def dict_gen_one(curs):
    field_names = [d[0].lower() for d in curs.description]
    row = curs.fetchone()
    if row:
        return dict(zip(field_names, row))
    return None


def dict_gen_many(curs):
    field_names = [d[0].lower() for d in curs.description]
    while True:
        rows = curs.fetchmany()
        if not rows:
            return
        for row in rows:
            yield dict(zip(field_names, row))


def encrypt(txt, keyfile=None, key=None):
    """Symmetric encryption using NaCl library."""
    # return ""
    # import nacl.utils
    # key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    if not key:
        if not keyfile:
            keyfile = current_app.config["SYMMETRIC_KEYFILE"]
        with open(keyfile, "rb") as f:
            key = f.read()
    box = nacl.secret.SecretBox(key)
    encryptedbytes = box.encrypt(txt.encode("utf-8"))
    return encryptedbytes


def decrypt(bytestring, keyfile=None, key=None):
    """Symmetric decryption using NaCl library."""
    # return ""
    if not key:
        if not keyfile:
            keyfile = current_app.config["SYMMETRIC_KEYFILE"]
        with open(keyfile, "rb") as f:
            key = f.read()
    box = nacl.secret.SecretBox(key)
    try:
        plaintext = box.decrypt(bytestring)
        return plaintext.decode("utf-8")
    except Exception as e:
        print(e)
        return ""


def get_base_name(apikey):
    db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        + " LEFT JOIN ScraperFields ON scraperid = scraperkey"
        + " WHERE dbaseapikey=%s"
        + " ORDER BY scraperkey, tablename, sortorder LIMIT 1",
        (apikey,),
    )
    row = dict_gen_one(c)

    return row.get("dbasename") if row else None


def get_scraper_definition(api_key: str):
    database = get_db()
    c = database.cursor()
    c.execute("SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (api_key,))

    data = dict_gen_one(c)
    c.close()

    return data


def generate_airtable_schema(apikey, db=None):
    if not db:
        db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        + " LEFT JOIN ScraperFields ON scraperid = scraperkey"
        + " WHERE dbaseapikey=%s"
        + " ORDER BY scraperkey, tablename, sortorder",
        (apikey,),
    )

    secrettoken = None
    scrapers = {}
    for rec in dict_gen_many(c):
        if not secrettoken:
            secrettoken = rec["secrettoken"]
        if rec["scrapername"] not in scrapers:
            scrapers[rec["scrapername"]] = {}
            scrapers[rec["scrapername"]]["id"] = rec["scraperkey"]

            if rec["data_table"]:
                scrapers[rec["scrapername"]][rec["data_table"]] = OrderedDict()
                scrapers[rec["scrapername"]][rec["data_table"]]["GroupBy"] = rec[
                    "data_groupby"
                ]
                scrapers[rec["scrapername"]][rec["data_table"]]["KeyField"] = rec[
                    "data_keyfield"
                ]
            if rec["group_table"]:
                scrapers[rec["scrapername"]][rec["group_table"]] = OrderedDict()
                scrapers[rec["scrapername"]][rec["group_table"]]["KeyField"] = rec[
                    "group_keyfield"
                ]
        if rec["tablename"] not in scrapers[rec["scrapername"]]:
            # data_table or group_table from SQL table "scrapers" does not match tablename from SQL table "scraperfields"
            # probably should throw a data error, but also this shouldn't happen
            print(
                "ERROR: Scraper definition has mismatched keys; '{}' doesn't match '{}' or '{}'.".format(
                    rec["tablename"], rec["data_table"], rec["group_table"]
                )
            )
            print("       Creating key, but this may have unexpected effects.")
            scrapers[rec["scrapername"]][rec["tablename"]] = OrderedDict()
        if rec["fieldlabel"] not in scrapers[rec["scrapername"]][rec["tablename"]]:
            scrapers[rec["scrapername"]][rec["tablename"]][rec["fieldlabel"]] = rec[
                "fieldname"
            ]
    c.close()
    return (scrapers, secrettoken)


def get_schemas_from_api_key(api_key: str) -> dict:
    schemas, _ = generate_airtable_schema(api_key)

    return schemas


def get_schema_from_api_key(pattern: str, api_key: str) -> Tuple[dict, dict]:
    schemas = get_schemas_from_api_key(api_key)

    schema = schemas.get(pattern)

    if not schema:
        return ({}, {})

    schema_dict = {}

    for tablename, fieldlist in schema.items():
        if not isinstance(fieldlist, dict):
            continue

        if "GroupBy" in fieldlist:
            schema_dict["low_remapper"] = fieldlist
            schema_dict["low_fields"] = list(fieldlist.values())
            schema_dict["low_table"] = tablename
            schema_dict["low_group_by"] = fieldlist["GroupBy"]
        else:
            schema_dict["high_remapper"] = fieldlist
            schema_dict["high_fields"] = list(fieldlist.values())
            schema_dict["high_table"] = tablename

    return schema, schema_dict


def get_airtable_pattern(apikey, scraperid, db=None, validateuserid=None):
    return _get_airtable_pattern_by_name_or_id(
        apikey,
        scraperid=scraperid,
        scrapername=None,
        db=db,
        validateuserid=validateuserid,
    )


def get_airtable_pattern_by_name(apikey, scrapername, db=None, validateuserid=None):
    return _get_airtable_pattern_by_name_or_id(
        apikey, scrapername=scrapername, db=db, validateuserid=validateuserid
    )


def _get_airtable_pattern_by_name_or_id(
    apikey, scraperid=None, scrapername=None, db=None, validateuserid=None
):
    """
    Returns a DataScraper object.
    """
    if not scraperid and not scrapername:
        return None

    if not db:
        db = get_db()
    c = db.cursor()
    if validateuserid:
        sql = ""
        sql += "SELECT * FROM AirTableAccounts"
        sql += " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        sql += " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        sql += " LEFT JOIN ScraperFields ON scraperid = scraperkey"
        sql += " WHERE userkey=%s AND dbaseapikey=%s"
        if scraperid:
            c.execute(
                sql + " AND scraperid=%s",
                (
                    validateuserid,
                    apikey,
                    scraperid,
                ),
            )
        elif scrapername:
            c.execute(
                sql + " AND scrapername=%s",
                (
                    validateuserid,
                    apikey,
                    scrapername,
                ),
            )
    else:
        sql = ""
        sql += "SELECT * FROM AirTableDatabases"
        sql += " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        sql += " LEFT JOIN ScraperFields ON scraperid = scraperkey"
        sql += " WHERE dbaseapikey=%s"
        if scraperid:
            c.execute(
                sql + " AND scraperid=%s",
                (
                    apikey,
                    scraperid,
                ),
            )
        elif scrapername:
            c.execute(
                sql + " AND scrapername=%s",
                (
                    apikey,
                    scrapername,
                ),
            )

    #    def DataScraper.__init__(self, apikey, name, tablename, groupby, groupname, tabledata={}, groupdata={}, dbid=None):
    name = None
    tblname = None
    tblkey = None
    tblgrpby = None
    grpname = None
    grpkey = None
    grpsorttable = None
    grpsortcolumn = None
    grpsortname = None
    encryptedtoken = ""
    tbldata = {}
    grpdata = {}
    for rec in dict_gen_many(c):
        """Each record returned (example):
        (Does not include optional data from AirTableAccounts, including "secrettoken")
        { 'dbaseid': 1, 'airtableaccountkey': 1, 'dbasename': 'Sample AirTable', 'dbaseapikey': 'app2n9fqx4VVNF4mM',
          'scraperid': 1, 'dbasekey': 1, 'scrapername': 'Fields', 'data_table': 'Field', 'data_keyfield': 'ID', 'data_groupby': 'CRM Class',
          'group_table': 'CRM Class', 'group_keyfield': 'ID', 'scraperfieldid': 1, 'scraperkey': 1, 'sortorder': 1, 'tablename': 'Field',
          'fieldlabel': 'Weight', 'fieldname': 'Weight', 'sortable': 1, groupable: 0, hideable: 0 }
        """
        name = rec["scrapername"]
        tblname = rec["data_table"]
        tblkey = rec["data_keyfield"]
        tblgrpby = rec["data_groupby"]
        grpname = rec["group_table"]
        grpkey = rec["group_keyfield"]
        grpsorttable = rec["group_sorttable"] if rec["group_sorttable"] else ""
        grpsortcolumn = rec["group_sortcolumn"] if rec["group_sortcolumn"] else ""
        grpsortname = rec["group_sortname"] if rec["group_sortname"] else ""
        encryptedtoken = rec["secrettoken"] if "secrettoken" in rec else ""

        if (
            rec["tablename"] != rec["data_table"]
            and rec["tablename"] != rec["group_table"]
        ):
            # DATA MISMATCH ERROR
            raise Exception(
                f'Data mismatch; "tablename": ({rec["tablename"]}) must be equal to "data_table" ({rec["data_table"]}) or "group_table" ({rec["group_table"]})'
            )

        if rec["tablename"] == rec["data_table"]:
            tbldata[rec["fieldlabel"]] = {
                "name": rec["fieldname"],
                "sortable": (rec["sortable"] == True),
                "groupable": (rec["groupable"] == True),
                "hideable": (rec["hideable"] == True),
                "function": rec["function"],
                "link": rec["link"] if rec["link"] else "",
                "exportable": (rec["exportable"] == True),
            }
        if rec["tablename"] == rec["group_table"]:
            grpdata[rec["fieldlabel"]] = {
                "name": rec["fieldname"],
                "sortable": (rec["sortable"] == True),
                "groupable": (rec["groupable"] == True),
                "hideable": (rec["hideable"] == True),
                "function": rec["function"],
                "link": rec["link"] if rec["link"] else "",
                "exportable": (rec["exportable"] == True),
            }
    c.close()
    if name and tblname:
        return DataScraper(
            apikey,
            name,
            tblname,
            tblkey,
            tblgrpby,
            grpname,
            grpkey,
            grpsorttable,
            grpsortcolumn,
            grpsortname,
            tabledata=tbldata,
            groupdata=grpdata,
            encryptedtoken=encryptedtoken,
            dbid=scraperid,
        )
    return None


def new_airtable_pattern(apikey, validateuserid, db=None):
    """
    Returns a DataScraper object.
    """
    if not db:
        db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " WHERE userkey=%s AND dbaseapikey=%s",
        (
            validateuserid,
            apikey,
        ),
    )
    data = dict_gen_one(c)
    c.close()

    if data is None:
        data = {}

    #    def DataScraper.__init__(self, apikey, name, tablename, groupby, groupname, tabledata={}, groupdata={}, dbid=None):
    return DataScraper(
        apikey,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        encryptedtoken=data.get("secrettoken"),
    )


def set_airtable_pattern(datascraper, forcepermission=False, db=None):
    """
    Takes a DataScraper object.
    """
    if not db:
        db = get_db()
    c = db.cursor()

    if not datascraper.dbid:
        # CREATE a new scraper, for which we need the apikey
        c.execute(
            "SELECT * FROM AirTableAccounts"
            + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
            + " WHERE dbaseapikey=%s",
            (datascraper.apikey,),
        )
        airtable = dict_gen_one(c)

        if airtable is not None and (
            forcepermission or permission(user=airtable["userkey"])
        ):
            c.execute(
                """INSERT INTO Scrapers
                (dbasekey, scrapername, data_table, data_keyfield, data_groupby, group_table, group_keyfield, group_sorttable, group_sortcolumn, group_sortname) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    airtable["dbaseid"],
                    datascraper.name,
                    datascraper.data_table,
                    datascraper.data_keyfield,
                    datascraper.data_groupby,
                    datascraper.group_table,
                    datascraper.group_keyfield,
                    datascraper.group_sorttable,
                    datascraper.group_sortcolumn,
                    datascraper.group_sortname,
                ),
            )
            # TODO: Could create duplicates this way?
            c.execute(
                "SELECT scraperid FROM Scrapers WHERE dbasekey=%s AND scrapername=%s",
                (
                    airtable["dbaseid"],
                    datascraper.name,
                ),
            )
            datascraper.dbid = c.fetchone()[0]
        else:
            return False
    # Get the conjoined data that links the scraper ID to a user, for permission
    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        + " WHERE scraperid=%s",
        (datascraper.dbid,),
    )
    scraper = dict_gen_one(c)
    if scraper is not None and (forcepermission or permission(user=scraper["userkey"])):
        # Must use SQL UPDATE because we want to keep the Scraper auto-incremenet field the same
        c.execute("START TRANSACTION")
        try:
            c.execute(
                """UPDATE Scrapers SET
                scrapername=%s, data_table=%s, data_keyfield=%s, data_groupby=%s, group_table=%s, group_keyfield=%s, group_sorttable=%s, group_sortcolumn=%s,group_sortname=%s
                WHERE scraperid=%s""",
                (
                    datascraper.name,
                    datascraper.data_table,
                    datascraper.data_keyfield,
                    datascraper.data_groupby,
                    datascraper.group_table,
                    datascraper.group_keyfield,
                    datascraper.group_sorttable,
                    datascraper.group_sortcolumn,
                    datascraper.group_sortname,
                    datascraper.dbid,
                ),
            )
            c.execute(
                "DELETE FROM ScraperFields WHERE scraperkey=%s", (datascraper.dbid,)
            )
            for i, (key, val) in enumerate(datascraper.Data.items(), start=1):
                c.execute(
                    "INSERT INTO ScraperFields"
                    + " (scraperkey, sortorder, tablename, fieldlabel, fieldname, sortable, groupable, hideable, function, link, exportable) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        datascraper.dbid,
                        i,
                        datascraper.data_table,
                        key,
                        val["name"],
                        val["sortable"],
                        val["groupable"],
                        val["hideable"],
                        val["function"],
                        val["link"] if val["link"] else None,
                        val["exportable"],
                    ),
                )
            for i, (key, val) in enumerate(datascraper.Group.items(), start=1):
                c.execute(
                    "INSERT INTO ScraperFields"
                    + " (scraperkey, sortorder, tablename, fieldlabel, fieldname, sortable, groupable, hideable, function, link, exportable) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        datascraper.dbid,
                        i,
                        datascraper.group_table,
                        key,
                        val["name"],
                        val["sortable"],
                        val["groupable"],
                        val["hideable"],
                        val["function"],
                        val["link"] if val["link"] else None,
                        val["exportable"],
                    ),
                )
            c.execute("COMMIT")
        except:
            print("Failed; rolling back")
            c.execute("ROLLBACK")
            raise
        # c.execute('COMMIT')
        # c.execute('ROLLBACK')
    else:
        return False
    return True


def permission(user):
    """TODO: all of it."""
    return True
