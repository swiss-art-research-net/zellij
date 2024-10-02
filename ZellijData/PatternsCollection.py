"""
Created on Mar. 15, 2021

@author: Pete Harris
"""

import html

from rdflib.plugins.parsers.notation3 import BadSyntax

# from ZellijData.TurtleCodeBlock import TurtleCodeBlock
from ZellijTable.RDFCodeBlock import RDFCodeBlock


class PatternsCollection(object):
    """
    A collection of named Patterns (typically Fields, Collections, Model, but could add more or less)
    and the data that was retrieved for them from the source (typically an AirTable).



    aggregate = {
        (numeric field identifier; i.e. 1): {
            "Name": (name; i.e. "Activity"),
            "Description": (description; i.e. "Projects, Research Tasks, Experiments and similar activities."),
            "Identifier": (code+numeric; i.e. "LAM.1"),
            "ID": (identifier+name; i.e. "[LAM.1] Activity"),
            "Fields": (dict of dictionaries) {
                (numeric field indentifier; i.e. 4): {
                    "Field": (full codes+names; i.e. "[LAM.1] Activity_[LAF.4] Name_label"),
                    "Field Name": (name; i.e. "Name_label"),
                    "Field Identifier": (sortable code; i.e. "LAF.4"),
                    "Description": (description; i.e. "This field is used to record the string value of the machine readable...", etc.),
                    "CRM Path": (CRM path; i.e. "->p1->E33_41[4_1]->rdfs:label->rdf:literal"),
                    "Turtle": (TurtleCodeBlock(), i.e. "<https://linked.art/example/models/activity> a ...", etc.)
                }
            },
            "Turtle": (TurtleCodeBlock(), i.e. "@prefix sari: <http://www.sari.com> .", etc.),
            "RDF": (RDFCodeBlock(), i.e. a parsed RDF Graph object)
        }
    }

    """

    # TODO: Check out "Person"; it does display JSON-LD but does not display proper Turtle.
    #        It reports an error instead, or perhaps a warning. Need to deal.

    def __init__(self, schema=None):
        """
        Can be passed a dict of Pattern Fields, which map our names to the fields in the database.
        #TODO: is this necessary, or helpful in any way? Maybe they don't.
        PATTERNS SCHEMA
        ---------------
        {
            'Collections': {
                'Collection_fields': {
                    'GroupBy': 'Collection',
                    'Turtle RDF': 'Total_Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Element name (from Field)',
                    'Identifier': 'ID (from Field)'
                },
                'Collection': {
                    'Turtle RDF': 'Turtle RDF',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            },
            'Fields': {
                'Fields': {
                    'GroupBy': 'CRM Class',
                    'Turtle RDF': 'Total Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Suggested Name',
                    'Identifier': 'Identifier'
                },
            },
            'Model': {
                'Model_fields': {
                    'GroupBy': 'Model',
                    'Turtle RDF': 'Model_Fields_Total_Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Field Name',
                    'Identifier': 'Name'
                },
                'Model': {
                    'Turtle RDF': 'Model_Turtle_Prefix',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            }
        }
        """
        if schema:
            pass
