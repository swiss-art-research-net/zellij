"""
Created on Mar. 9, 2021

@author: Pete Harris
"""

import re
import html


class TurtleCodeBlock(object):
    """
    A small data structure for handling Turtle (Terse RDL Triple Language) code.
    Additive; can build up Turtle from components.
    https://www.w3.org/TR/turtle/
    """

    def __init__(self, *args):
        """
        Constructor
        """
        self.originaltext = ""
        if len(args) > 0:
            self.originaltext = "\n".join(args)
        self.prefix = []
        self.body = []
        self.warnings = None
        self.errors = None
        for arg in args:
            self.add(arg)

    def add(self, text):
        """Set the warnings and errors to empty lists. If they are None then code block hasn't yet been parsed."""
        if self.warnings is None:
            self.warnings = []
        if self.errors is None:
            self.errors = []

        for i, ln in enumerate(text.splitlines(), start=1):
            m = re.match(r"^\s*(\@prefix .*?\s+\<[^>]*\>\s+\.)\s*?(.*)", ln)
            if m:
                if m.group(1) and not m.group(1) in self.prefix:
                    t = m.group(1).rstrip()
                    if t != m.group(1):
                        self.warnings.append(
                            {"line": i, "text": "Trailing whitespace."}
                        )
                    self.prefix.append(t)
                if m.group(2):
                    self.warnings.append(
                        {"line": i, "text": "Missing newline between prefix and body."}
                    )
                    t = self._addLine(i, m.group(2))
                    if t:
                        self.body.append(t)
            else:
                t = self._addLine(i, ln)
                if t:
                    self.body.append(t)

    def _addLine(self, ln, text):
        if text.strip == "":
            # skip empty lines
            return
        t = text.rstrip()
        if t != text:
            self.warnings.append({"line": ln, "text": "Trailing whitespace."})
        m = re.search(r"\<\s*\>", t)
        if m:
            # Empty URL reference will get replaced with local file name when passed to RDF.
            # Must fill it with something that includes a transport protocol, and also throw a warning.
            t = re.sub(r"\<\s*\>", "<http://UNKNOWN>", t)
            self.warnings.append({"line": ln, "text": "Empty URL reference < >"})
        return t

    def html(self):
        return html.escape(self.stringify())

    def text(self):
        return self.stringify()

    def stringify(self):
        txt = ""
        txt += "\n".join(self.prefix)
        if self.prefix:
            txt += "\n\n"
        txt += "\n".join(self.body)
        return txt

    def __str__(self):
        return f"<Turtle_RDF: {self.linecount()} lines>"

    def __repr__(self):
        return f"<Turtle_RDF: {self.linecount()} lines>"

    def linecount(self):
        return len(self.prefix) + len(self.body)
