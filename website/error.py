"""
Created on Mar. 23, 2021

@author: Pete Harris
"""

import traceback

import nacl.secret
from flask import Blueprint, current_app, render_template

bp = Blueprint("error", __name__)


@bp.app_errorhandler(500)
def handle_server_error_webpage(error):
    return render_template(
        "error/servererror.html",
        error=error,
        estring=str(error),
        traceback=htmlFormatTraceback(traceback.format_exc()),
    )


def htmlFormatTraceback(trace):
    out = {"error": "", "codefile": "", "codefunction": "", "codeline": "", "code": ""}
    err = ""
    code = ""
    file = ""

    codefile = ""
    codeline = ""
    codefunc = ""
    for i, ln in enumerate(reversed(trace.split("\n"))):
        if i == 1:
            err = ln
            out["error"] = err
        elif i % 2 == 0:
            code = ln
            out["code"] = code
        elif i % 2 == 1:
            file = ln
            out["codefile"] = file
            if current_app.config["APP_PROJECT_FOLDER"] in file:
                codefile, codeline, codefunc = file.split(",")
                codefunc = codefunc[4:]
                codeline = codeline[6:]
                codefile = codefile.split(current_app.config["APP_PROJECT_FOLDER"])[1][
                    1:-1
                ]
                out["codefunction"] = codefunc
                out["codeline"] = codeline
                out["codefile"] = codefile
                out["errorstring"] = (
                    err
                    + "\n"
                    + codefile
                    + "\n"
                    + codefunc
                    + "\n"
                    + codeline
                    + "\n"
                    + code
                )
                out["obfuscated"] = obfuscate(out["errorstring"])
                out["reconstituted"] = deobfuscate(out["obfuscated"])
                return out
    out["errorstring"] = (
        err + "\n" + codefile + "\n" + codefunc + "\n" + codeline + "\n" + code
    )
    out["obfuscated"] = obfuscate(out["errorstring"])
    out["reconstituted"] = deobfuscate(out["obfuscated"])
    return out


def obfuscate(txt):
    """Symmetric encryption using NaCl library."""
    # import nacl.utils
    # key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    key = current_app.config["ERROR_OBFUSCATOR_SYMMETRIC_KEY"]
    box = nacl.secret.SecretBox(key)
    encryptedbytes = box.encrypt(txt.encode("utf-8"))
    return " ".join("{:02X}".format(x) for x in encryptedbytes)


def deobfuscate(txt):
    """Symmetric decryption using NaCl library."""
    key = current_app.config["ERROR_OBFUSCATOR_SYMMETRIC_KEY"]
    box = nacl.secret.SecretBox(key)
    hx = txt.split(" ")
    bstr = bytes.fromhex("".join(hx))
    try:
        plaintext = box.decrypt(bstr)
        return plaintext.decode("utf-8")
    except Exception:
        return ""


"""
@bp.route("/", methods=["GET"])
def main():
    try:
        db = get_db()
        flash("get_db() works.")
    except Exception as e:
        flash("get_db() failed: " + str(e) )
    try:
        c = db.cursor()
        flash( "db.cursor() works." )
    except Exception as e:
        flash( "db.cursor() failed: " + str(e) )

    c.close()
    return render_template("menubase.html")
"""
