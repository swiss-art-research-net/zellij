"""
Created on Mar. 16, 2021

@author: Pete Harris
"""

import functools
from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from website.db import get_db, dict_gen_one, dict_gen_many

bp = Blueprint("user", __name__, url_prefix="/user")


@bp.route("/create", methods=["GET", "POST"])
def create():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        db = get_db()
        c = db.cursor()
        error = None

        if not username:
            error = "Username is required."
        elif not password:
            error = "Password is required."
        else:
            c.execute("SELECT userid FROM Users WHERE username=%s", (username,))
            r = dict_gen_one(c)
            if r is not None:
                error = "User {} is already registered.".format(username)

        if error is None:
            c.execute(
                "INSERT INTO Users (username, password) VALUES (%s,%s)",
                (username, generate_password_hash(password)),
            )
            db.commit()
            c.close()
            return redirect(url_for("user.login"))

        c.close()
        flash(error)

    return render_template("user/create.html")


@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        error = None
        db = get_db()
        c = db.cursor()
        c.execute("SELECT * FROM Users WHERE username=%s", (username,))
        user = dict_gen_one(c)
        c.close()
        if user is None:
            error = "Incorrect username."
        elif not check_password_hash(user["password"], password):
            error = "Incorrect password."

        if error is None:
            session.clear()
            session["user_id"] = user["userid"]
            return redirect(url_for("index"))

        flash(error)

    return render_template("user/login.html")


@bp.route("/edit", methods=["GET", "POST"])
def edit():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        db = get_db()
        c = db.cursor()
        error = None

        if not username:
            error = "Username is required."
        elif not password:
            error = "Password is required."
        else:
            if username != g.user["username"]:
                c.execute("SELECT userid FROM Users WHERE username=%s", (username,))
                r = dict_gen_one(c)
                if r is not None:
                    error = "User name {} is already in use.".format(username)

        if error is None:
            c.execute(
                "UPDATE Users SET username=%s, password=%s" + " WHERE userid=%s",
                (
                    username,
                    generate_password_hash(password),
                    g.user["userid"],
                ),
            )
            db.commit()
            c.close()
            return redirect(url_for("user.login"))

        c.close()
        flash(error)

    return render_template("user/edit.html", original=g.user)


@bp.before_app_request
def load_logged_in_user():
    user_id = session.get("user_id")
    if user_id is None:
        g.user = None
    else:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT * FROM Users WHERE userid=%s", (user_id,))
        g.user = dict_gen_one(c)
        c.close()


@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for("user.login"))
        return view(**kwargs)

    return wrapped_view
