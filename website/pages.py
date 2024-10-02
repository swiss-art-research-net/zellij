"""
Created on Mar. 11, 2021

@author: Pete Harris
"""

from flask import Blueprint, render_template


bp = Blueprint("pages", __name__)


@bp.route("/", methods=["GET"])
def mainpage():
    return render_template("big.html")
