from flask import render_template, Blueprint, request
from rdflib.plugins.parsers.notation3 import BadSyntax

from CRITERIA import criteria
from ZellijData.RDFCodeBlock import RDFCodeBlock
from ZellijData.TurtleCodeBlock import TurtleCodeBlock
from pyairtable.formulas import EQUAL, OR, STR_VALUE, match


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


def display_graph(prefix, input, item, categories):
    graphs = {}
    allturtle = "\n".join(input)
    # identifiers = []
    category_field = categories.get(prefix)
    # for i in range(len(matching_group[1])):
    #     identifiers.append(matching_group[1][i]['Field'][0])
    # fields = airtable.get_multiple_records_by_formula("Field",
    # OR(
    #                         *list(
    #                             map(
    #                                 lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
    #                                 identifiers,
    #                             )
    #                         )
    #                     ),
    # )
    # categories = {}

    # for field in fields:
    #     field_data = field.get('fields', {})
        
    #     if 'Collection_Deployed' in field_data:
    #         if isinstance(field_data['Collection_Deployed'], str):
    #             name = field_data['Collection_Deployed']
    #         else:
    #             name = field_data['Collection_Deployed'][0]
            
    #         if name[:3] == "rec":
    #             name = airtable.get_record_by_id("Collection", name)['fields']['UI_Name'] + ":Sample"
    #     else:
    #         name = field_data.get('UI_Name') + ":Sample"

    #     if name:
    #         field_id = field.get('id', '')  
    #         field_ui_name = field_data.get('UI_Name', '')  # Get UI name or default to ""
            
    #         if name not in categories:
    #             categories[name] = []  # Change to a list of dicts
            
    #         categories[name].append({"id": field_id, "ui_name": field_ui_name})

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

    return render_template("functions/display_graph.html", prefix=prefix, graphs=graphs, categories=category_field)


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
