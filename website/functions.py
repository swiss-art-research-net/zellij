from flask import render_template, Blueprint, request
from rdflib.plugins.parsers.notation3 import BadSyntax

from CRITERIA import criteria
from ZellijData.RDFCodeBlock import RDFCodeBlock
from ZellijData.TurtleCodeBlock import TurtleCodeBlock


bp = Blueprint("functions", __name__, url_prefix="/functions")


@bp.route("/ontology", methods=["POST"])
def generate_ontology_graph():
    rdf = RDFCodeBlock(request.form["turtle_text"])

    try:
        return criteria.ontology(rdf.turtle())
    except Exception as e:
        return "ERROR: " + str(e)


@bp.route("/instance", methods=["POST"])
def generate_instance_graph():
    rdf = RDFCodeBlock(request.form["turtle_text"])

    try:
        return criteria.instance(rdf.turtle())
    except Exception as e:
        return str(e)


@bp.route("/jsonld", methods=["POST"])
def generate_json_ld():
    rdf = RDFCodeBlock(request.form["turtle_text"])

    try:
        return rdf.jsonld()
    except Exception as e:
        print(e)
        return str(e)


def display_graph(prefix, input, item):
    graphs = {}
    allturtle = "\n".join(input)

    TurtlePrefix = ""
    if "Turtle RDF" in item.ExtraFields:
        turtle_prefix = TurtleCodeBlock(item.ExtraFields["Turtle RDF"])
        TurtlePrefix = "\n".join(turtle_prefix.prefix)

    allturtle = TurtlePrefix + "\n\n" + allturtle
    turtle = TurtleCodeBlock(allturtle)
    graphs["turtle_text"] = turtle.text()
    try:
        rdf = RDFCodeBlock(turtle.text())
        graphs["rdf"] = rdf
    except BadSyntax as bs:
        rdf = str(bs)
        graphs["error"] = rdf

    if "error" in graphs:
        graphs["turtle"] = rdf
    else:
        graphs["turtle"] = rdf.turtle()

    graphs["generateOntology"] = generate_ontology_graph
    graphs["generateInstance"] = generate_instance_graph
    graphs["generateJsonLD"] = generate_json_ld

    return render_template("functions/display_graph.html", prefix=prefix, graphs=graphs)


# each function should have a label and a function
# the label is what is displayed in the dropdown
# the function is the function that is called when the label is selected
# the function should take a prefix and a list of input strings and the item as arguments
# the function should return a template
functions = {
    "graph_display": {
        "label": "Graph Display",
        "function": display_graph,
    }
}
