"""
Created on Mar. 9, 2021

@author: Pete Harris
"""

import re
import os.path
import json

# import sys
from io import StringIO
import html
from rdflib import Graph
from rdflib.term import URIRef
# from rdflib_jsonld.context import Context
# from rdflib.plugins.parsers.notation3 import BadSyntax


class RDFCodeBlock(object):
    """
    A small data structure for handling RDF code.
    More generic, but defaults to our expected case, which is
    TuRTLe (Terse RDF Triple Language) code.
    https://www.w3.org/TR/turtle/
    Along with the possibility to output other code formats,
    specifically JSON-LD.
    https://json-ld.org/
    """

    DEFAULT_STYLE = "turtle"

    def __init__(self, *args, style=DEFAULT_STYLE):
        """
        Constructor
        """
        self.originaltext = None
        self.style = style
        self.graph = None
        self.originaltext = "\n".join(args)
        self.warnings = None
        self.errors = None
        if len(args) > 0:
            self.parse(self.originaltext, self.style)
            # except BadSyntax as e:
        """
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Getty-linked-art.json"), "r") as f:
            self.jsoncontext = json.load(f)
        """
        self.jsoncontext = "https://linked.art/ns/v1/linked-art.json"

    def parse(self, text, style=DEFAULT_STYLE):
        reader = StringIO(text)
        self.graph = Graph()
        self.graph.parse(reader, format=style)
        self._validate_uri()

    def _validate_uri(self):
        _invalid_uri_chars = '<>" {}|\\^`'
        err = []
        for g in self.graph:
            for ref in g:
                if isinstance(ref, URIRef):
                    for c in _invalid_uri_chars:
                        if c in str(ref):
                            if not str(ref) in err:
                                err.append(str(ref))
                            break
        if err:
            self.warnings = err

    def print(self, style=DEFAULT_STYLE, context=None):
        try:
            if not context:
                context = self.jsoncontext
            out = self.graph.serialize(format=style, context=context)
            out = out  # .decode("utf-8")
            return out
        except Exception as e:
            return str(e)

    def html(self, style=DEFAULT_STYLE):
        return html.escape(self.print(style=style))

    # named helper functions, for being too lazy to pass "style".
    def jsonld(self):
        return self.print(style="json-ld")

    def turtle(self):
        return self.print(style="turtle")

    def __str__(self):
        return self.print()

    def sanitizeTurtle(self, text):
        """Set the warnings and errors to empty lists. If they are None then code block hasn't yet been parsed."""
        if self.warnings is None:
            self.warnings = []

        prefix = []
        body = []
        for i, ln in enumerate(text.splitlines(), start=1):
            m = re.match(r"^\s*(\@prefix .*?\s+\<[^>]*\>\s+\.)\s*?(.*)", ln)
            if m:
                if m.group(1) and not m.group(1) in prefix:
                    t = m.group(1).rstrip()
                    if t != m.group(1):
                        self.warnings.append(
                            {"line": i, "text": "Trailing whitespace."}
                        )
                    prefix.append(t)
                if m.group(2):
                    self.warnings.append(
                        {"line": i, "text": "Missing newline between prefix and body."}
                    )
                    t = m.group(2).rstrip()
                    if t != m.group(2):
                        self.warnings.append(
                            {"line": i, "text": "Trailing whitespace."}
                        )
                    body.append(t)
            else:
                t = ln.rstrip()
                if t != ln:
                    self.warnings.append({"line": i, "text": "Trailing whitespace."})
                body.append(t)
        txt = ""
        txt += "\n".join(prefix)
        if prefix:
            txt += "\n"
        txt += "\n".join(body)
        return txt
