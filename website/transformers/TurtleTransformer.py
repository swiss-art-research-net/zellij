from rdflib import Graph, URIRef, BNode, Literal, RDF
from rdflib.namespace import Namespace

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")


class TurtleTransformer:
    @staticmethod
    def transform(field_name: str, input_str: str):
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
                if parts[idx + 1] == "rdf:literal":
                    graph.add((URIRef(uris[idx-1]), CRM[current_part], Literal(field_name.replace(" ", "_")+"_value")))
                else:
                    graph.add((URIRef(uris[idx-1]), CRM[current_part], URIRef(uris[idx+1])))
            else:
                graph.add((URIRef(uris[idx]), RDF.type, CRM[current_part.split('[')[0]]))

        print(graph.serialize(format="turtle"))
