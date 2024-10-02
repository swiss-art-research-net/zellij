from rdflib.plugins.serializers.turtle import TurtleSerializer

SUBJECT = 0
VERB = 1
OBJECT = 2

class TurtleSerializerCustom(TurtleSerializer):
    indentString = " "
