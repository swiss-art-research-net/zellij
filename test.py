"""
Created on Mar. 8, 2021

@author: Pete Harris
"""

from gettext import lngettext
import re
import requests
import io

from ZellijTable.AggregatePlusFieldData import AggregatePlusFieldData
# from ZellijData.ComposeDocument import ComposeDocument

airtableAPI = "appXap4TfbrRkcRSH"
# Yes, this is insecure!!! Just testing.
bearerToken = "keyez539eiGXV36lc"


if __name__ == "__main__":
    AGGREGATE_FIELD_NAMES = [
        "Record ID",  # actual AirTable ID; i.e. "rec1XmKWwQc9jWjw4"
        "Identifier",  # an indexable code; i.e. "LAM.12"
        "Name",  # descriptive name; i.e. "Provenance Activity"
        "Full Name",  # combined identifiers and name; i.e. "[LAM.12] Provenance Activity"
        "Description",  # text description
        "Turtle",  # Turtle RDF description code
    ]
    SUB_FIELD_NAMES = [
        "Record ID",  # actual AirTable ID; i.e. "recYom3WYtiCTMTs0"
        "Parent ID",  # actual AirTable ID of parent; i.e. "rec1XmKWwQc9jWjw4"
        "Identifier",  # an indexable code; i.e. "LAF.4"
        "Name",  # a descriptive name; i.e. "Name_label"
        "Full Name",  # combined identifiers and name; i.e. "[LAM.12] Provenance Activity_[LAP.4] Name_label"
        "Description",  # text description
        "CRM Path",  # CRM path; i.e. "->p1->E33_41[4_1]->rdfs:label->rdf:literal"
        "Turtle",  # Turtle RDF description code
    ]

    data = {
        "aggregate": {
            "Table": "Model",
            "Identifier": "Identifier",
            "Name": "Name",
            "Full Name": "ID",
            "Description": "Description",
            "Turtle": "Model_Turtle_Prefix",
        },
        "fields": {
            "Table": "Model_Fields",
            "Parent ID": "Model",
            "Identifier": "Field Identifier",
            "Name": "Field Name",
            "Full Name": "Name",
            "Description": "Description",
            "CRM Path": "CRM Path",
            "Turtle": "Model_Fields_Total_Turtle",
        },
    }
    """
    obj = AggregatePlusFieldCollector(data)
    dataobj = obj.get()
    mkmaker = ComposeDocument()
    print( mkmaker.composeHTML(dataobj) )
    """

    rdfHead = """@prefix sari: <http://www.sari.com> .
@prefix crmdig: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix frbr: <http://iflastandards.info/ns/fr/frbr/frbroo/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix la: <http://linked.art> .
@prefix dcterms: <http://decterms> .
@prefix dc: <http://dc> .
@prefix skos: <http://skos> .
"""
    rdfBody = """
<https://linked.art/example/models/activity> a <http://www.cidoc-crm.org/cidoc-crm/E7_Activity>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/4_1> .
<https://linked.art/example/conceptual_object/4_1> a crm:E33_E41_Linguistic_Appellation;
rdfs:label "Name_label_value" .
<https://linked.art/example/models/activity> a <http://www.cidoc-crm.org/cidoc-crm/E7_Activity>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/4_1> .
<https://linked.art/example/conceptual_object/4_1> a crm:E33_E41_Linguistic_Appellation;
crm:P67i_is_referred_to_by <https://linked.art/example/textual_object/44_1> .
<https://linked.art/example/textual_object/44_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/models/activity> a <http://www.cidoc-crm.org/cidoc-crm/E7_Activity>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/4_1> .
<https://linked.art/example/conceptual_object/4_1> a crm:E33_E41_Linguistic_Appellation;
crm:P190_has_symbolic_content "Name_string_value" .
<https://linked.art/example/models/activity> a <http://www.cidoc-crm.org/cidoc-crm/E7_Activity>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/4_1> .
<https://linked.art/example/conceptual_object/4_1> a crm:E33_E41_Linguistic_Appellation;
crm:P72_has_language <https://linked.art/example/type/7_1> .
<https://linked.art/example/type/7_1> a crm:E56_Language .
<https://linked.art/example/models/activity> a <http://www.cidoc-crm.org/cidoc-crm/E7_Activity>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/4_1> .
<https://linked.art/example/conceptual_object/4_1> a crm:E33_E41_Linguistic_Appellation;
crm:P2_has_type <http://vocab.getty.edu/page/aat/300404670> .
<http://vocab.getty.edu/page/aat/300404670> a crm:E55_Type;
rdfs:label "preferred terms" .
"""
    rdfInput = rdfHead + rdfBody

    from ZellijTable.RDFCodeBlock import RDFCodeBlock

    tmp = RDFCodeBlock(rdfInput)
    print(tmp.jsonld())
    # import tempfile
    # tfile = tempfile.NamedTemporaryFile()
    # criteria.ontologyGenerator(rdfInput, tfile)
    # tfile = "tempfile.mmd"

    # TODO:    RDFlib wants a stream, whether that's a file or a file-like object, according to
    #          https://rdflib.readthedocs.io/en/stable/intro_to_parsing.html
    # But Criteria is guessing format based on filename I guess? Needs debugging.

    """
    criteria.ontology(io.StringIO(rdfInput), tfile)
    with open(tfile) as f:
        print(f.read())
    """
