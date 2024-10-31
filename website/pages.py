"""
Created on Mar. 11, 2021

@author: Pete Harris
"""

from flask import Blueprint, render_template

from website.db import dict_gen_many, get_db


bp = Blueprint("pages", __name__)


@bp.route("/", methods=["GET"])
def mainpage():
    return render_template("big.html")

@bp.route("/about", methods=["GET"])
def about():
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

    grouped_dbs = {}

    for db in dblist:
        if db["accountname"] not in grouped_dbs:
            grouped_dbs[db["accountname"]] = []
        grouped_dbs[db["accountname"]].append(db)

    return render_template("about-page.html", projects=list(grouped_dbs.keys()))
