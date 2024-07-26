from rdflib import Graph, URIRef, BNode, Literal, RDF
from rdflib.namespace import Namespace

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")


class TurtleTransformer:
    input_str: str

    def __init__(self, input_str: str):
        self.input_str = input_str

    def transform(self):
        input_str = "E7_Entity->P1_is_identified_by->E42_Identifier[8_1]->P2_has_type->E55_Type[9_1]"
        print(input_str.split("->"))
        graph = Graph()
        graph.bind("crm", CRM)
        graph.bind("rdf", RDF)

        parts = input_str.lstrip("->").split("->")
        uris = {}
        for idx in range(len(parts)):
            part = parts[idx]
            if idx % 2 == 0:
                if "[" in part:
                    identifiers = part.split('[')[1].split(']')[0]
                    uris[idx] = f"https://linked.art/example/{identifiers}"
                else:
                    uris[idx] = f"https://linked.art/example/{part}"

        for idx in range(len(parts)):
            current_part = parts[idx]

            if idx % 2 == 1:
                graph.add((URIRef(uris[idx-1]), CRM[current_part], URIRef(uris[idx+1])))
            else:
                graph.add((URIRef(uris[idx]), RDF.type, CRM[current_part.split('[')[0]]))

        print(graph.serialize(format="turtle"))
