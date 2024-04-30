"""
Created on Apr. 22, 2021

@author: Pete Harris
"""
from flask import Blueprint, flash, render_template


bp = Blueprint("testpages", __name__, url_prefix="/testpages")


@bp.route("/flash", methods=["GET", "POST"])
def flashtest():
    flash("Error <i>one</i>.", "error")
    flash("Error <i>two</i>.", "error")
    flash("Warning <i>one</i>.", "warning")
    flash("Warning <i>two</i>.", "warning")
    flash("Info, just <i>one</i>.", "info")
    flash("Uncategorized message.")
    return render_template("development/testflash.html")
