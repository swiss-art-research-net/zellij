"""
Created on Mar. 16, 2021

@author: Pete Harris
"""
import html
import json
import re
from collections import OrderedDict

from flask import Blueprint, flash, g, redirect, render_template, request, url_for
from werkzeug.security import generate_password_hash

# from ZellijTable.AggregateDataCollector import AggregateDataCollector
from ZellijData.AirTableConnection import EnhancedResponse, AirTableConnection
from website.DataScraper import DataScraper
from website.auth import login_required
from website.db import (
    get_db,
    dict_gen_one,
    dict_gen_many,
    decrypt,
    encrypt,
    set_airtable_pattern,
)
from website.functions import functions

bp = Blueprint("datasources", __name__, url_prefix="/datasources")


@bp.route("/connections", methods=["GET"])
@login_required
def connections():
    db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        + " WHERE userkey=%s",
        (g.user["userid"],),
    )
    accounts = {}
    for dt in dict_gen_many(c):
        if dt["accountid"] not in accounts:
            accounts[dt["accountid"]] = {
                "accountid": dt["accountid"],
                "accountname": dt["accountname"],
                "secrettoken": dt["secrettoken"],
                "airtables": {},
            }
        if dt["dbaseapikey"]:
            if dt["dbaseapikey"] not in accounts[dt["accountid"]]["airtables"]:
                accounts[dt["accountid"]]["airtables"][dt["dbaseapikey"]] = {
                    "dbaseid": dt["dbaseid"],
                    "dbasename": dt["dbasename"],
                    "dbaseapikey": dt["dbaseapikey"],
                    "scrapers": {},
                }
        if dt["scraperid"]:
            accounts[dt["accountid"]]["airtables"][dt["dbaseapikey"]]["scrapers"][
                dt["scraperid"]
            ] = dt
    c.close()
    return render_template("generator/scraperlist.html", accounts=accounts)


# filter scrappers
@bp.route("/connections/<scrapername>", methods=["GET", "POST"])
def filteredScrapper(scrapername):
    db = get_db()
    c = db.cursor()
    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " LEFT JOIN Scrapers ON dbaseid = dbasekey"
        + " WHERE userkey=%s AND scrapername = %s",
        (
            g.user["userid"],
            scrapername,
        ),
    )
    accounts = {}
    for dt in dict_gen_many(c):
        if dt["accountid"] not in accounts:
            accounts[dt["accountid"]] = {
                "accountid": dt["accountid"],
                "accountname": dt["accountname"],
                "secrettoken": dt["secrettoken"],
                "airtables": {},
            }
        if dt["dbaseapikey"]:
            if dt["dbaseapikey"] not in accounts[dt["accountid"]]["airtables"]:
                accounts[dt["accountid"]]["airtables"][dt["dbaseapikey"]] = {
                    "dbaseid": dt["dbaseid"],
                    "dbasename": dt["dbasename"],
                    "dbaseapikey": dt["dbaseapikey"],
                    "scrapers": {},
                }
        if dt["scraperid"]:
            accounts[dt["accountid"]]["airtables"][dt["dbaseapikey"]]["scrapers"][
                dt["scraperid"]
            ] = dt
    c.close()
    return render_template("generator/scraperlist.html", accounts=accounts)


# <scraperid>
@bp.route("/airtable/scraper/<apikey><scraperid>", methods=["GET", "POST"])
@login_required
def displayScraper(apikey):
    scraperid = request.args.get("scraper")
    db = get_db()
    c = db.cursor()

    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " WHERE userkey=%s AND dbaseapikey=%s",
        (
            g.user["userid"],
            apikey,
        ),
    )
    base = dict_gen_one(c)

    prefill = DataScraper.load(apikey, scraperid, validateuserid=g.user["userid"])

    return render_template(
        "generator/scraperdisplay.html",
        scraper=scraperid,
        apikey=apikey,
        base=base,
        prefill=prefill,
    )


# paste outside of a scrapper inside the main manage data sources page
@bp.route("/connnections2/<dbId>/<scraperId>", methods=["GET", "POST"])
@login_required
def pasteInManageDataSources(dbId, scraperId):
    db = get_db()
    c = db.cursor()

    c.execute(
        """
        SELECT * FROM Scrapers
        WHERE scraperid = %s
    """,
        (scraperId,),
    )
    existing_scraper = dict_gen_one(c)

    if existing_scraper is None:
        return redirect(url_for("datasources.connections"))

    c.execute(
        """
        INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_keyfield, data_groupby, group_table, group_keyfield, group_sorttable, group_sortcolumn, group_sortname)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            dbId,
            existing_scraper["scrapername"],
            existing_scraper["data_table"],
            existing_scraper["data_keyfield"],
            existing_scraper["data_groupby"],
            existing_scraper["group_table"],
            existing_scraper["group_keyfield"],
            existing_scraper["group_sorttable"],
            existing_scraper["group_sortcolumn"],
            existing_scraper["group_sortname"],
        ),
    )
    db.commit()

    inserted_id = c.lastrowid

    c.execute(
        """
        SELECT * FROM ScraperFields
        WHERE scraperkey = %s
        """,
        (scraperId,),
    )
    existing_fields = dict_gen_many(c)
    db2 = get_db()
    cursor2 = db2.cursor()
    for field in existing_fields:
        cursor2.execute(
            f"""
            INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname, sortable, groupable, hideable)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                inserted_id,
                field["sortorder"],
                field["tablename"],
                field["fieldlabel"],
                field["fieldname"],
                field["sortable"],
                field["groupable"],
                field["hideable"],
            ),
        )
        db2.commit()

    return redirect(url_for("datasources.connections"))


@bp.route("/connections2/<apikey>", methods=["GET", "POST"])
@login_required
def displayScraper2(apikey):
    """Load all the scraper details from the database first, so it's available to the entire rest of the function."""
    scraperid = request.args.get("scraper")
    error = None

    # Get information about this scrapers' context; i.e. where it's from. Includes the user's secret Bearer Token.
    db = get_db()
    c = db.cursor()
    c.execute(
        "SELECT * FROM AirTableAccounts"
        + " LEFT JOIN AirTableDatabases ON accountid = airtableaccountkey"
        + " WHERE userkey=%s AND dbaseapikey=%s",
        (
            g.user["userid"],
            apikey,
        ),
    )
    base = dict_gen_one(c)
    c.close()

    if base is None:
        base = {}

    encryptedtoken = base["secrettoken"] if "secrettoken" in base else ""

    # This needs to happen first, as sometimes we're deleting a record that caused an error in the loading DataScaper code.
    if request.method == "POST":
        if request.form.get("deleter") == "confirm":
            c = db.cursor()
            # use this to confirm it's one of ours, and eligible to be deleted
            if base:
                c.execute("START TRANSACTION")
                try:
                    c.execute(
                        "DELETE FROM ScraperFields WHERE scraperkey=%s", (scraperid,)
                    )
                    c.execute("DELETE FROM Scrapers WHERE scraperid=%s", (scraperid,))
                    c.execute("COMMIT")
                except:
                    print("Failed; rolling back")
                    c.execute("ROLLBACK")
                    c.close()
                    raise
                c.close()
            return redirect(url_for("datasources.connections"))
    # end Confirm Delete

    try:
        prefill = DataScraper.load(apikey, scraperid, validateuserid=g.user["userid"])
        if not prefill:
            prefill = DataScraper.new(apikey, g.user["userid"])
    except Exception as e:
        return render_template(
            "generator/scrapererror.html", scraperid=scraperid, base=base, error=e
        )

    if request.method == "POST":
        # update "prefill" with data from actual form, even if this isn't the final submit.
        # need to do this before scanning for samples, in case the table names have changed.
        print("prefill print", prefill)
        _update_DataScraper_with_post_fields(prefill, request)

    # See if we can load some samples / validation data. Requires a valid Bearer Token for AirTable.
    datasamples = None
    dataerrors = None
    groupsamples = None
    grouperrors = None
    if encryptedtoken:
        secret = decrypt(encryptedtoken)
        connection = AirTableConnection(secret, apikey)
        if prefill.data_table:
            validatedata = connection.getsinglerecord(prefill.data_table, {})

            datasamples = dict()
            for k, v in validatedata["fields"].items():
                datasamples[k] = _cleanseSampleData(v)

        if prefill.group_table:
            validategroup = connection.getsinglerecord(prefill.group_table, {})

            groupsamples = dict()
            for k, v in validategroup["fields"].items():
                groupsamples[k] = _cleanseSampleData(v)

    # now let's finish up processing the POST
    if request.method == "POST":
        # clear button
        if request.form.get("groupScraperTrash") == "trash":
            return render_template(
                "generator/scrapereditor2.html",
                scraperid="",
                base="",
                prefill="",
                datasamples=[],
                groupsamples=[],
                dataerror="",
                grouperror="",
                delete="",
                functions=functions,
            )

        if request.form.get("deleter") == "delete":
            return render_template(
                "generator/scrapereditor2.html",
                scraperid=scraperid,
                base=base,
                prefill=prefill,
                datasamples=datasamples,
                groupsamples=groupsamples,
                dataerror=dataerrors,
                grouperror=grouperrors,
                delete="confirm",
                functions=functions,
            )

        if (
            request.form.get("submitter") == "save"
            or request.form.get("submitter") == "create"
        ):
            if set_airtable_pattern(prefill):
                flash("Save successful.", "info")
            else:
                flash("Save failed.", "error")

        if request.form.get("submitter") == "Paste":
            db = get_db()
            c = db.cursor()

            query = """
                    SELECT `dbaseid`
                    FROM `AirTableDatabases`
                    WHERE dbaseapikey = %s
            """

            c.execute(query, (apikey,))
            dbkeytemp = c.fetchall()

            query = """
                UPDATE Scrapers
                SET dbasekey = %s
                ORDER BY scraperid DESC
                LIMIT 1;
            """
            c.execute(query, (dbkeytemp,))
            db.commit()

        return render_template(
            "generator/scrapereditor2.html",
            scraperid=scraperid,
            base=base,
            prefill=prefill,
            datasamples=datasamples,
            groupsamples=groupsamples,
            dataerror=dataerrors,
            grouperror=grouperrors,
            functions=functions,
        )
    # end POST

    if error:
        flash(error, "error")

    return render_template(
        "generator/scrapereditor2.html",
        scraperid=scraperid,
        base=base,
        prefill=prefill,
        datasamples=datasamples,
        groupsamples=groupsamples,
        dataerror=dataerrors,
        grouperror=grouperrors,
        functions=functions,
    )


def get_prefill(api_key, scraper_id):
    data_scraper = DataScraper.load(api_key, scraper_id, validateuserid=None)

    if data_scraper is None:
        return "default"

    group_sort = {
        "table": data_scraper.group_sorttable,
        "order": data_scraper.group_sortcolumn,
        "name": data_scraper.group_sortname,
    }

    if not all(group_sort.values()):
        group_sort = None

    return (
        data_scraper.Data,
        data_scraper.Group,
        group_sort,
    )


def _update_DataScraper_with_post_fields(ds, request):
    """
    DataScraper.__init__(self, apikey, name, tablename, tablekeyfield, groupby, groupname, groupkeyfield, tabledata={}, groupdata={}, encryptedtoken=None, dbid=None):

    """
    ds.name = request.form["scrapername"]
    ds.data_table = request.form["data_table"]
    ds.data_keyfield = request.form["data_keyfield"]
    ds.data_groupby = request.form["data_groupby"]
    ds.group_sorttable = (
        request.form.get("group_sorttable")
        if request.form.get("group_sorttable")
        else None
    )
    ds.group_sortcolumn = (
        request.form.get("group_sortcolumn")
        if request.form.get("group_sortcolumn")
        else None
    )
    ds.group_sortname = (
        request.form.get("group_sortname")
        if request.form.get("group_sortname")
        else None
    )
    ds.group_table = request.form["group_table"]
    ds.group_keyfield = request.form["group_keyfield"]
    sortstuff = {"data": {}, "group": {}}
    for itemkey, itemval in request.form.items():
        m = re.match(
            r"(data|group)(sort|nom|val|sorter|grouper|hider|link|function|exporter)_(\d+)",
            itemkey,
        )
        if m:
            if m.group(3) not in sortstuff[m.group(1)]:
                sortstuff[m.group(1)][m.group(3)] = [
                    None,
                    None,
                    None,
                ]  # Leave sort index as None to provoke an error in the sort if it's blank, as that's bad code elsewhere.
            if m.group(2) == "sort":
                sortstuff[m.group(1)][m.group(3)][0] = int(itemval)
            if m.group(2) == "nom":
                sortstuff[m.group(1)][m.group(3)][1] = itemval
            if (
                m.group(2) == "val"
                or m.group(2) == "sorter"
                or m.group(2) == "grouper"
                or m.group(2) == "hider"
            ) and not sortstuff[m.group(1)][m.group(3)][2]:
                sortstuff[m.group(1)][m.group(3)][2] = {}
            if m.group(2) == "val":
                sortstuff[m.group(1)][m.group(3)][2]["name"] = itemval
            if m.group(2) == "sorter":
                sortstuff[m.group(1)][m.group(3)][2]["sortable"] = itemval == "on"
            if m.group(2) == "grouper":
                sortstuff[m.group(1)][m.group(3)][2]["groupable"] = itemval == "on"
            if m.group(2) == "function":
                sortstuff[m.group(1)][m.group(3)][2]["function"] = itemval
            if m.group(2) == "exporter":
                sortstuff[m.group(1)][m.group(3)][2]["exporter"] = itemval == "on"
            if m.group(2) == "link":
                sortstuff[m.group(1)][m.group(3)][2]["link"] = (
                    itemval if itemval else ""
                )
            if m.group(2) == "hider":
                sortstuff[m.group(1)][m.group(3)][2]["hideable"] = itemval == "on"

    # This sort may not be necessary, because it appears that the form is returned in DOM order, so it already takes into account the re-sorting.
    # But better safe than sorry. Might not be true in all browsers.
    ds.Data = OrderedDict()
    for item in sorted(sortstuff["data"].items(), key=lambda x: x[1][0]):
        ds.addDataItem(
            item[1][1],
            item[1][2]["name"],
            sortable=item[1][2]["sortable"] if "sortable" in item[1][2] else False,
            groupable=item[1][2]["groupable"] if "groupable" in item[1][2] else False,
            hideable=item[1][2]["hideable"] if "hideable" in item[1][2] else False,
            function=item[1][2]["function"] if "function" in item[1][2] else None,
            link=item[1][2]["link"] if "link" in item[1][2] else "",
            exportable=item[1][2]["exporter"] if "exporter" in item[1][2] else False,
        )
    ds.Group = OrderedDict()
    for item in sorted(sortstuff["group"].items(), key=lambda x: x[1][0]):
        ds.addGroupItem(
            item[1][1],
            item[1][2]["name"],
            sortable=item[1][2]["sortable"] if "sortable" in item[1][2] else False,
            groupable=item[1][2]["groupable"] if "groupable" in item[1][2] else False,
            hideable=item[1][2]["hideable"] if "hideable" in item[1][2] else False,
            function=item[1][2]["function"] if "function" in item[1][2] else None,
            link=item[1][2]["link"] if "link" in item[1][2] else "",
            exportable=item[1][2]["exporter"] if "exporter" in item[1][2] else False,
        )


def _cleanseSampleData(d):
    if isinstance(d, str) and "\n" in d:
        return "“" + html.escape(d.splitlines()[0]) + "”..."
    if isinstance(d, list):
        tmp = []
        for i, s in enumerate(d):
            if i == 3:
                tmp.append("...")
                break

            if isinstance(s, int):
                s = str(s)

            if s[:3] == "rec" and len(s) == 17:
                tmp.append("<var>&lt;record id:" + s[3:] + "&gt;</var>")
            else:
                tmp.append("“" + html.escape(s) + "”")
        if len(tmp) == 1:
            return tmp[0]
        return "[" + ", ".join(tmp) + "]"
    return d


@bp.route("/airtables/account", methods=["GET", "POST"])
@bp.route("/airtables/account/<accountid>", methods=["GET", "POST"])
@login_required
def airTableAccountCreateEdit(accountid=None):
    db = get_db()
    c = db.cursor()
    error = None

    if request.method == "POST":
        name = request.form["ataccountname"]
        secret = request.form["secret"]

        if not name:
            error = "Name is required."
        elif not secret:
            error = "Secret bearer token is required."
        elif not accountid:
            c.execute(
                "SELECT accountid FROM AirTableAccounts WHERE accountname=%s", (name,)
            )
            r = dict_gen_one(c)
            if r is not None:
                error = "AirTable account {} is already registered.".format(name)

        if error is None:
            if accountid:
                c.execute(
                    "UPDATE AirTableAccounts SET userkey=%s, accountname=%s, secrettoken=%s"
                    + " WHERE accountid=%s",
                    (
                        g.user["userid"],
                        name,
                        encrypt(secret),
                        accountid,
                    ),
                )
            else:
                c.execute(
                    "INSERT INTO AirTableAccounts (userkey, accountname, secrettoken)"
                    + " VALUES (%s,%s,%s)",
                    (
                        g.user["userid"],
                        name,
                        encrypt(secret),
                    ),
                )

            db.commit()
            c.close()
            return redirect(url_for("datasources.connections"))

        flash(error, "error")

    existing = None
    if accountid:
        c.execute("SELECT * FROM AirTableAccounts WHERE accountid=%s", (accountid,))
        r = dict_gen_one(c)

        if r is None:
            flash(f"Account with id {accountid} does not exist", "error")
            c.close()
            return render_template("generator/accounteditor.html", existing=existing)

        secrethash = generate_password_hash(decrypt(r["secrettoken"]))
        existing = {
            "accountid": r["accountid"],
            "userkey": r["userkey"],
            "accountname": r["accountname"],
            "secrethash": secrethash,
        }
    c.close()
    return render_template("generator/accounteditor.html", existing=existing)


@bp.route("/airtables/account/<accountid>", methods=["DELETE"])
@login_required
def airTableAccountDelete(accountid):
    db = get_db()
    c = db.cursor()

    c.execute("DELETE FROM AirTableAccounts WHERE accountid=%s", (accountid,))
    c.execute("DELETE FROM AirTableDatabases WHERE airtableaccountkey=%s", (accountid,))
    db.commit()

    return json.dumps({"success": True}), 200, {"ContentType": "application/json"}


@bp.route("/airtables/database/<accountid>/<dbaseid>", methods=["DELETE"])
@login_required
def airTableDatabaseDelete(accountid, dbaseid):
    db = get_db()
    c = db.cursor()

    c.execute("DELETE FROM AirTableDatabases WHERE dbaseid=%s", (dbaseid,))
    db.commit()

    return json.dumps({"success": True}), 200, {"ContentType": "application/json"}


@bp.route("/airtables/database/<accountid>", methods=["GET", "POST"])
@bp.route("/airtables/database/<accountid>/<dbaseid>", methods=["GET", "POST"])
@login_required
def airTableDatabaseCreateEdit(accountid=None, dbaseid=None):
    db = get_db()
    c = db.cursor()
    error = None

    if request.method == "POST":
        name = request.form["apiname"]
        key = request.form["apikey"]

        if not name:
            error = "A descriptive name is required."
        elif not key:
            error = "API Key is required."
        elif not key:
            c.execute(
                "SELECT dbaseapikey FROM AirTableDatabases WHERE dbaseid=%s", (dbaseid,)
            )
            r = dict_gen_one(c)
            if r is not None:
                error = "AirTable API key {} already exists.".format(key)

        if error is None:
            if dbaseid:
                c.execute(
                    "UPDATE AirTableDatabases SET dbasename=%s, dbaseapikey=%s"
                    + " WHERE dbaseid=%s",
                    (
                        name,
                        key,
                        dbaseid,
                    ),
                )
            else:
                c.execute(
                    "INSERT INTO AirTableDatabases (airtableaccountkey, dbasename, dbaseapikey)"
                    + " VALUES (%s,%s,%s)",
                    (
                        accountid,
                        name,
                        key,
                    ),
                )

            db.commit()
            c.close()
            return redirect(url_for("datasources.connections"))

        flash(error, "error")

    existing = None
    if dbaseid:
        c.execute("SELECT * FROM AirTableDatabases WHERE dbaseid=%s", (dbaseid,))
        existing = dict_gen_one(c)
    c.close()
    return render_template("generator/airdbeditor.html", existing=existing)
