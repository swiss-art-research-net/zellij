'''
Created on Mar. 15, 2021

@author: Pete Harris
'''
import html

from rdflib.plugins.parsers.notation3 import BadSyntax

from ZellijTable.PatternItem import PatternItem
from ZellijTable.TurtleCodeBlock import TurtleCodeBlock
from ZellijTable.RDFCodeBlock import RDFCodeBlock

class PatternObject(object):
    '''
    A collection of data belonging to a single Pattern.
    
    '''
    
    # TODO: Check out "Person"; it does display JSON-LD but does not display proper Turtle.
    #        It reports an error instead, or perhaps a warning. Need to deal.

    def __init__(self, name):
        '''
        Will contain the data from a single pattern, which is a set of fields detailing the low-level ontology,
        grouped by a mandatory grouping field which can link to an optional group-level ontology.
        
        Currently stored in a dict of AggregateDataObjecs.
        '''
        self.name = name
        self.patternItems = dict()  # list of PatternItem objects
        
    def __str__(self):
        return "<Pattern> " + self._text()
    
    def __repr__(self):
        return "<ZellijData.PatternObject " + self._text() + ">"
    
    def _text(self):
        """ Decided against using “curved quotes” to support limited consoles. """
        txt = ""
        txt += '"' + self.name + '"'
        txt += " (" + str(len(self.patternItems))
        subs = 0
        for sub in self.patternItems.values():
            subs += len(sub.GroupedData())
        txt += ":" + str(subs) + ")"
        return txt
    
    # Define the Dictionary iterator functions
    def items(self):
        for k, v in self.patternItems.items():
            yield (k, v)
    
    def keys(self):
        for k in self.patternItems.keys():
            yield k
    
    def values(self):
        for v in self.patternItems.values():
            yield v
        
    
    def addGroup(self, indata):
        '''
        SAMPLE INPUT:
        indata = {
            '<record_id>': {
                'Identifier': 'E42'.
                'Name': 'E42 Indentifier',
                'Description': 'You know, a description',
                'Turtle RDF': '<https://linked.art/example/actor/E74> a <http://www.cidoc-crm.org/cidoc-crm/E74_Group>;'
            }, ... etc. ...
        }
        '''
        if isinstance(indata, dict):
            for k, v in indata.items():
                if k not in self.patternItems:
                    self.patternItems[k] = PatternItem(data = v)
    
    def addData(self, indata):
        for k, d in indata.items():
            idx = d["GroupBy"]
            if idx not in self.patternItems:
                self.patternItems[idx] = PatternItem()
            self.patternItems[idx].addFields(k, d)
    
    def generateGraphs(self):
        for po in self.patternItems.values():
            po.generateTurtle()
            po.generateRDF()
    
    def listGroupFields(self):
        firstkey = next(iter(self.patternItems))
        return [x for x in self.patternItems[firstkey].keys() if not x in [PatternObject.GROUPED_DATA_FIELD]]
    
    def listGroupFieldsTabular(self):
        firstkey = next(iter(self.patternItems))
        return [x for x in self.patternItems[firstkey].keys() if not x in [PatternObject.GROUPED_DATA_FIELD, "Turtle RDF"]]
    
    def listFields(self):
        firstkey = next(iter(self.patternItems))
        secondkey = next(iter(self.patternItems[firstkey][PatternObject.GROUPED_DATA_FIELD]))
        return [x for x in self.patternItems[firstkey][PatternObject.GROUPED_DATA_FIELD][secondkey].keys() if not x in ["GroupBy"]]
    
    def listFieldsTabular(self):
        firstkey = next(iter(self.patternItems))
        secondkey = next(iter(self.patternItems[firstkey][PatternObject.GROUPED_DATA_FIELD]))
        return [x for x in self.patternItems[firstkey][PatternObject.GROUPED_DATA_FIELD][secondkey].keys() if not x in ["GroupBy", "Turtle RDF"]]
    
    