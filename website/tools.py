"""
Created on Jun. 9, 2021

@author: Pete Harris
"""
import os.path
import html
import tempfile
import re
import json
from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
    current_app,
    send_file,
)
from werkzeug.utils import secure_filename

from website.auth import login_required

from rdflib import Graph

from rdflib.plugins.parsers.notation3 import BadSyntax
from shutil import make_archive, rmtree

bp = Blueprint("tools", __name__)


@bp.route("/jsonconverter", methods=["GET", "POST"])
@login_required
def jsonconverter():
    if request.method == "POST":
        toplevel = None
        fh = None
        if "file" in request.files:
            # submitted using the file button; now we may check its validity
            file = request.files["file"]
            if file.filename == "":
                flash("No file selected")
                return redirect(request.url)
            if (
                "." not in file.filename
                or file.filename.rsplit(".", 1)[1].lower()
                not in current_app.config["ALLOWED_UPLOAD_EXTENSIONS"]
            ):
                flash("Not a legal file type")
                return redirect(request.url)
            localfilename = secure_filename(file.filename)
            localpath = os.path.join(current_app.config["UPLOAD_FOLDER"], localfilename)
            file.save(localpath)
            fh = open(localpath, "r", encoding="utf-8")

        elif "rdftext" in request.form:
            # You need this saved as a file, because it will get loaded as a file after the "questions" page
            fh = tempfile.NamedTemporaryFile(
                mode="r+",
                encoding="utf-8",
                delete=False,
                dir=current_app.config["UPLOAD_FOLDER"],
            )
            localfilename = fh.name
            localpath = fh.name
            fh.write(request.form["rdftext"])
            fh.seek(0)

        elif "localpath" in request.form:
            # If localpath came back from the page, then this is the questions answered POST.
            localpath = request.form["localpath"]
            if not os.path.isfile(localpath):
                flash(f"Took too long; temporary file no longer exists. Start again.")
                return redirect(request.url)
            fh = open(localpath, "r", encoding="utf-8")
            toplevel = []
            for k, v in request.form.items():
                m = re.match(r"check\d+_(.*)", k)
                if m:
                    toplevel.append(m.group(1))

        if fh:
            success, output = _rdf_to_json(fh)
            if success:
                if not toplevel:
                    # Haven't asked for the top level types yet; do that now
                    js = json.loads(output)
                    categories = {}
                    for item in js["@graph"]:
                        if "type" in item:
                            categories[item["type"]] = (
                                1
                                if item["type"] not in categories
                                else categories[item["type"]] + 1
                            )
                    return render_template(
                        "tools/jsonquestions.html",
                        categories=categories,
                        localpath=localpath,
                    )
                else:
                    # now we can parse the JSON and save every top level result as a file to be zipped
                    # start with collecting the notes in a UUID dictionary
                    js = json.loads(output)
                    nodes = {item["id"]: item for item in js["@graph"]}
                    nicename = os.path.basename(request.form["localpath"]).rsplit(
                        ".", 1
                    )[0]
                    tmpdir = tempfile.mkdtemp()
                    targetdir = os.path.join(tmpdir, nicename)
                    os.makedirs(targetdir)
                    for toplevelcategory in toplevel:
                        toplevelobjects = [
                            v for k, v in nodes.items() if v["type"] == toplevelcategory
                        ]
                        for obj in toplevelobjects:
                            idee = obj["id"][9:]
                            tmpfile = idee + ".json"
                            fh = open(os.path.join(targetdir, tmpfile), "w+")
                            o = _buildRecursiveJson(obj, nodes)
                            if "@context" in js:
                                o["@context"] = js["@context"]
                            fh.write(json.dumps(o))
                            fh.close()
                    # Now all files are written; time to zip them up
                    outprefix = nicename + "_" if nicename else ""
                    tmparch = tempfile.mkstemp(
                        prefix=outprefix, dir=current_app.config["UPLOAD_FOLDER"]
                    )
                    zipfile = make_archive(tmparch[1], "zip", root_dir=targetdir)
                    justname = os.path.basename(zipfile)
                    rmtree(tmpdir)
                    return redirect(url_for("tools.jsonresult", filename=justname))

                # for item in js["@graph"]:
                #    if item["type"] == "Person":
                #        print("DEBUG:", item["_label"], item["id"])
                """
                # save contents of variable "output" as a file, for downloading
                outfh = tempfile.NamedTemporaryFile(prefix=outprefix, suffix=".json", mode="w+", encoding="utf-8", delete=False, dir=current_app.config["UPLOAD_FOLDER"])
                outfh.write(output)
                outfh.close()
                justname = os.path.basename(outfh.name)
                return redirect(url_for('tools.jsonresult', filename=justname))
                #return render_template("tools/jsonresult.html", json=output, filename=localfilename)
                """
            else:
                fh.close()
                return render_template("tools/jsonerror.html", error=output)
        else:
            flash("Nothing submitted")
            return redirect(request.url)
    # end POST

    return render_template("tools/jsonconverter.html")


def _buildRecursiveJson(src, allnodes, haskey=None):
    if isinstance(src, str):
        if src[:9] == "urn:uuid:":
            if haskey and haskey == "id":
                return None
            elif src in allnodes:
                return _buildRecursiveJson(allnodes[src], allnodes, haskey=haskey)
            else:
                print("WARNING: referencing an unknown UUID")
                return src
        else:
            return src
    if isinstance(src, list):
        for i, n in enumerate(src):
            src[i] = _buildRecursiveJson(n, allnodes)
    if isinstance(src, dict):
        # simplify special case
        if (
            haskey
            and haskey == "content"
            and "@language" in src
            and "@value" in src
            and len(src) == 2
        ):
            return src["@value"]
        # otherwise just iterate over dict
        for k, v in list(src.items()):
            src[k] = _buildRecursiveJson(v, allnodes, haskey=k)
            if src[k] is None:
                src.pop(k, None)
    return src


@bp.route("/jsonresult/<filename>", methods=["GET", "POST"])
@login_required
def jsonresult(filename):
    localpath = os.path.join(current_app.config["UPLOAD_FOLDER"], filename)

    if request.method == "POST":
        if "downloadbutton" in request.form:
            return send_file(
                localpath,
                as_attachment=True,
                attachment_filename=filename,
                mimetype="application/zip",
            )

    return render_template("tools/jsonresult.html", filename=filename)


def _rdf_to_json(filehandle):
    try:
        graph = Graph()
        graph.parse(filehandle, format="turtle")
        out = graph.serialize(
            format="json-ld", context="https://linked.art/ns/v1/linked-art.json"
        )
        out = out.decode("utf-8")
        return True, out

    except BadSyntax as bs:
        return False, {"error": bs, "pretty": formatRDFerror(bs)}
    except Exception as e:
        return False, {"error": e}


def formatRDFwarnings(warn):
    """
    "warn" is just a list of straight text from STDERR, as issued by the rdflib.
    """
    htm = ""
    for w in warn:
        htm += f"""<div><span class="badlink">{w}</span> does not look like a valid URI, trying to serialize this will break.</div>\n"""
    return htm


def formatRDFerror(err, name=None):
    """
    self._str = argstr.encode('utf-8')  # Better go back to strings for errors
    self._i = i
    self._why = why
    self.lines = lines
    self._uri = uri
    """
    print("DEBUG:tools:89:", err)
    # develop snippet
    # TODO: should make the lookback and lookahead into callable subroutines that can specify number of lines.
    s = err._str.decode("utf-8")
    i = int(err._i)
    pre = []
    post = []
    ln0begin = s[0:i].rfind("\n")
    ln0len = s[ln0begin + 1 : -1].find("\n")
    errline = (str(err.lines + 1), s[ln0begin + 1 : ln0begin + 1 + ln0len])
    ln1begin = ln0begin + 1 + ln0len
    ln1len = s[ln1begin + 1 : -1].find("\n")
    post.append((str(err.lines + 2), s[ln1begin + 1 : ln1begin + 1 + ln1len]))
    ln_1begin = s[0:ln0begin].rfind("\n")
    ln_1len = s[ln_1begin + 1 : -1].find("\n")
    pre.append((str(err.lines), s[ln_1begin + 1 : ln_1begin + 1 + ln_1len]))
    indent = i - ln0begin

    htm = '<p class="error">When trying to parse this RDF, the code reported the following error:</p>\n'
    htm += "<pre>\n"
    txt = "ERROR" + (' in "' + name + '"' if name else "") + ":\n"
    for x in pre:
        htm += (
            '<span class="linenumber">  '
            + x[0].ljust(4)
            + "│ </span>"
            + html.escape(x[1])
            + "\n"
        )
        txt += "  " + x[0].ljust(4) + "│ " + x[1] + "\n"
    htm += (
        '<span class="linenumber">  '
        + errline[0].ljust(4)
        + "│ </span>"
        + html.escape(errline[1])
        + "\n"
    )
    txt += "  " + errline[0].ljust(4) + "│ " + errline[1] + "\n"
    htm += '<span class="errorline">' + "┌".ljust(7 + indent, "─") + "┘" + "</span>\n"
    txt += "┌".ljust(7 + indent, "─") + "┘" + "\n"
    for x in post:
        htm += (
            '<span class="errorline">│ </span><span class="linenumber">'
            + x[0].ljust(4)
            + "│ </span>"
            + html.escape(x[1])
            + "\n"
        )
        txt += "│ " + x[0].ljust(4) + "│ " + x[1] + "\n"
    htm += '<span class="errorline">│</span>\n'
    txt += "│\n"
    htm += (
        '<span class="errorline">╘═</span><span class="error"> Bad syntax ('
        + err._why
        + ")</span>\n"
    )
    txt += "╘═ Bad syntax (" + err._why + ")\n"
    htm += "</pre>\n"
    htm += '<p class="error">Consequently, we are unable to generate JSON-LD.</p>\n'
    print(txt)
    return htm
