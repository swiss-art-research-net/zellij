from flask import render_template
from rdflib.plugins.parsers.notation3 import BadSyntax

from CRITERIA import criteria
from ZellijData.RDFCodeBlock import RDFCodeBlock
from ZellijData.TurtleCodeBlock import TurtleCodeBlock


def display_graph(prefix, input, item):
    graphs = {}
    allturtle = "\n".join(input)

    TurtlePrefix = ""
    if "Turtle RDF" in item.ExtraFields:
        turtle_prefix = TurtleCodeBlock(item.ExtraFields["Turtle RDF"])
        TurtlePrefix = "\n".join(turtle_prefix.prefix)

    allturtle = TurtlePrefix + "\n\n" + allturtle
    turtle = TurtleCodeBlock(allturtle)
    try:
        rdf = RDFCodeBlock(turtle.text())
    except BadSyntax as bs:
        rdf = str(bs)
        graphs["error"] = rdf

    if "error" in graphs:
        graphs["turtle"] = rdf
    else:
        graphs["turtle"] = rdf.turtle()

    if "error" not in graphs:
        try:
            graphs["ontology"] = criteria.ontology(rdf.turtle())
        except Exception as e:
            graphs["ontology"] = str(e)

    if "error" not in graphs:
        try:
            graphs["instance"] = criteria.instance(rdf.turtle())
        except Exception as e:
            graphs["instance"] = str(e)

    if "error" not in graphs:
        try:
            graphs["jsonld"] = rdf.jsonld()
        except Exception as e:
            graphs["jsonld"] = str(e)

    graphs["rdf"] = rdf

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
