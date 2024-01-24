"""
Created on Mar. 15, 2021

@author: Pete Harris
"""

import logging

from rdflib.plugins.parsers.notation3 import BadSyntax

from CRITERIA import criteria
from ZellijData.RDFCodeBlock import RDFCodeBlock
from ZellijData.TurtleCodeBlock import TurtleCodeBlock
from website.tools import formatRDFwarnings, formatRDFerror

logging.basicConfig(level=logging.DEBUG)


class SingleGroupedItem(object):
    """
    Contains information for one Grouped Item, which consists of the data for one record of a grouping table,
    plus the collection of records from the underlying data tables that are grouped by the grouping record,
    plus an aggregated Turtle representation, plus an RDF object which has
    serialized that Turtle data and can validate and transform it.

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

    def __init__(self, data={}):
        """
        Constructor
        """
        self._GroupedData = dict()
        self._GroupedFields = dict()
        self.Identifier = ""
        self.Name = ""
        self.Description = ""
        self.ID = ""

        for k, v in data.items():
            if k == "ID":
                self.ID = v
            if k.upper() == "IDENTIFIER":
                self.Identifier = v
            if k.upper() == "NAME":
                self.Name = v
            if k.upper() == "DESCRIPTION":
                self.Description = v

        self.ExtraFields = data

        self.Turtle = None
        self.TurtlePrefix = None
        self.RDFcode = None
        self.RDFerror = None
        self.OntologyGraph = ""
        self.InstanceGraph = ""

    def __str__(self):
        txt = f'<Zellij.SingleGroupedItem "{self.ID}":{len(self._GroupedData)}>'
        return txt

    def __len__(self):
        return len(self._GroupedData)

    # Define the Dictionary iterator functions
    def items(self):
        for k, v in self._GroupedData.items():
            yield (k, self._stripbraces(v))

    def keys(self):
        for k in self._GroupedData.keys():
            yield k

    def values(self):
        for v in self._GroupedData.values():
            yield self._stripbraces(v)

    def GroupedData(self):
        return self._GroupedData

    def GroupedFields(self):
        return self._GroupedFields

    def addFields(self, key, data):
        d = {}
        for k, v in data.items():
            if not k in ["GroupBy"]:
                d[k] = v
        self._GroupedData[key] = d

    def generateTurtle(self):
        allturtle = "\n".join(
            [
                x["Turtle RDF"] if "Turtle RDF" in x else ""
                for x in self._GroupedData.values()
            ]
        )
        # logging.debug('*****eeeeeee******* %s', self.TurtlePrefix)
        if not self.TurtlePrefix:
            if "Turtle RDF" in self.ExtraFields:
                tmp = TurtleCodeBlock(self.ExtraFields["Turtle RDF"])
                self.TurtlePrefix = "\n".join(tmp.prefix)
        if self.TurtlePrefix:
            allturtle = self.TurtlePrefix + "\n\n" + allturtle
            # logging.debug('*****alt******* %s', allturtle)
        self.Turtle = TurtleCodeBlock(allturtle)
        return self.Turtle

    def generateRDF(self):
        try:
            self.RDFerror = None
            self.RDFcode = RDFCodeBlock(self.generateTurtle().text())
            self.generateOntologyGraph()
            self.generateInstanceGraph()
        except BadSyntax as bs:
            self.RDFcode = None
            self.RDFerror = self._formatRDFerror(bs)
            return self.RDFerror

    def generateOntologyGraph(self):
        if not self.RDFerror:
            try:
                self.OntologyGraph = criteria.ontology(self.RDFcode.turtle())
                # logging.debug('%s*****ontologygraph*******', self.OntologyGraph)

            except Exception as e:
                self.OntologyGraph = str(e)

    def generateInstanceGraph(self):
        if not self.RDFerror:
            try:
                self.InstanceGraph = criteria.instance(self.RDFcode.turtle())
            except Exception as e:
                self.InstanceGraph = str(e)

    def generateTurtleForPrefix(self, prefix):
        values = self._GroupedFields.get(prefix)
        if values is None:
            return ""

        allturtle = "\n".join(
            [x["Turtle RDF"] if "Turtle RDF" in x else "" for x in values]
        )
        # logging.debug('*****eeeeeee******* %s', self.TurtlePrefix)
        if not self.TurtlePrefix:
            if "Turtle RDF" in self.ExtraFields:
                tmp = TurtleCodeBlock(self.ExtraFields["Turtle RDF"])
                self.TurtlePrefix = "\n".join(tmp.prefix)
        if self.TurtlePrefix:
            allturtle = self.TurtlePrefix + "\n\n" + allturtle
            # logging.debug('*****alt******* %s', allturtle)
        turtle = TurtleCodeBlock(allturtle)
        return turtle

    def generateOntologyGraphForPrefix(self, prefix):
        values = self._GroupedFields.get(prefix)
        if values is None:
            return ""

        try:
            rdf = RDFCodeBlock(self.generateTurtleForPrefix(prefix).text())
            return criteria.ontology(rdf.turtle())
        except BadSyntax as bs:
            rdf = str(bs)
            return rdf

    def generateInstanceGraphForPrefix(self, prefix):
        values = self._GroupedFields.get(prefix)
        if values is None:
            return ""

        try:
            rdf = RDFCodeBlock(self.generateTurtleForPrefix(prefix).text())
            return criteria.ontology(rdf.turtle())
        except BadSyntax as bs:
            rdf = str(bs)
            return rdf

    def generateJsonLDForPrefix(self, prefix):
        values = self._GroupedFields.get(prefix)
        if values is None:
            return ""

        try:
            rdf = RDFCodeBlock(self.generateTurtleForPrefix(prefix).text())
            return rdf.jsonld()
        except BadSyntax as bs:
            rdf = str(bs)
            return rdf

    def rdf_warnings(self):
        if self.RDFcode and self.RDFcode.warnings:
            return self._formatRDFwarnings(self.RDFcode.warnings)
        return False

    def turtle(self):
        if not self.RDFerror:
            return self.RDFcode.turtle()
        else:
            return ""

    def is_rdferror(self):
        return self.RDFerror

    def is_jsonld(self):
        return not self.RDFerror

    def jsonld(self):
        if not self.RDFerror:
            return self.RDFcode.jsonld()
        else:
            return ""

    def jsonld_error(self):
        if self.RDFerror:
            return self.RDFerror
        else:
            return ""

    def _formatRDFwarnings(self, warn):
        """
        "warn" is just a list of straight text from STDERR, as issued by the rdflib.
        """
        return formatRDFwarnings(warn)

    def _formatRDFerror(self, err):
        """
        self._str = argstr.encode('utf-8')  # Better go back to strings for errors
        self._i = i
        self._why = why
        self.lines = lines
        self._uri = uri
        """
        return formatRDFerror(err, self.Name)

    def _stripbraces(self, val):
        """
        When an AirTable field is a referenced field, it appears as a one-entry list, which if left as-is
        will display in the Jinja template as "['actual text']" with the Python list representation. We
        have to convert them from one-entry lists to strings to make this stop.
        """
        if isinstance(val, list) and len(val) == 1:
            val = val[0]
        if isinstance(val, dict):
            for k, v in val.items():
                val[k] = self._stripbraces(v)
        return val
