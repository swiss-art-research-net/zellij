import unittest
import os.path
import requests

from ZellijTable.PatternObject import PatternObject
from ZellijTable.PatternItem import PatternItem


class TestPatternObject(unittest.TestCase):
    
    def setUp(self):
        self.pattern1 = PatternObject("Collections")
        self.group1_1 = {
            'rec2ixJtajtwFemo4': {
                'Turtle RDF': '@prefix sari: <http://www.sari.com> .\n@prefix crmdig: <http://www.cidoc-crm.org/cidoc-crm/> .\n\n<https://linked.art/example/event/215_1> a crm:E86_Leaving ;\ncrm:P144_joined_with <https://linked.art/example/actor/228_1> .\n<https://linked.art/example/actor/228_1> a crm:E74_Group .',
                'Description': '',
                'Name': 'Leaving Group',
                'Identifier': 'LAP.17'
            },
            'rec3ShvB8hEvXrVog': {
                'Turtle RDF': '@prefix sari: <http://www.sari.com> .\n@prefix crmdig: <http://www.cidoc-crm.org/cidoc-crm/> .\n@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .\n@prefix frbr: <http://iflastandards.info/ns/fr/frbr/frbroo/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix la: <http://linked.art> .\n@prefix dcterms: <http://decterms> .\n@prefix dc: <http://dc> .\n@prefix skos: <http://skos> .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P117_occurs_during\n<https://linked.art/example/event/333_2> .\n<https://linked.art/example/event/333_2> a\xa0\ncrm:E4_Period .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P2_has_type\n<https://linked.art/example/type/334_1> .\n<https://linked.art/example/type/334_1> a\xa0\ncrm:E55_Type .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P182_ends_or_before_or_with_the_start_of\n<https://linked.art/example/event/335_1> .\n<https://linked.art/example/event/335_1> a\xa0\ncrm:E4_Period .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P67i_is_referred_to_by\n<https://linked.art/example/conceptual_object/336_1> .\n<https://linked.art/example/conceptual_object/336_1> a\xa0\ncrm:E33_Linguistic_Object .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P4_has_time-span\n<https://linked.art/example/time_span/337_1> .\n<https://linked.art/example/time_span/337_1> a\xa0\ncrm:E52_Time-Span .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P182i_starts_after_or_with_the_end_of\n<https://linked.art/example/event/338_1> .\n<https://linked.art/example/event/338_1> a\xa0\ncrm:E4_Period .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P26_moved_to <https://linked.art/example/place/339_1> .\n<https://linked.art/example/place/339_1> a crm:E53_Place .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P25_moved <https://linked.art/example/physical_object/340_1> .\n<https://linked.art/example/physical_object/340_1> a crm:E22_Man_Made_Thing .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\nrdfs:label "Formation_label_value" .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P67i_is_referred_to_by\n<https://linked.art/example/conceptual_object/342_1> .\n<https://linked.art/example/conceptual_object/342_1> a\xa0\ncrm:E33_Linguistic_Object .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P1_is_identified_by\n<https://linked.art/example/name/343_1> .\n<https://linked.art/example/name/343_1> a\xa0\ncrm:E33_E41_Linguistic_Appellation .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P27_moved_from <https://linked.art/example/place/344_1> .\n<https://linked.art/example/place/344_1> a crm:E53_Place .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P14_carried_out_by\n<https://linked.art/example/actor/345_1> .\n<https://linked.art/example/actor/345_1> a\xa0\ncrm:E39_Actor .<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P1_is_identified_by\n<https://linked.art/example/identifier/346_1> .\n<https://linked.art/example/identifier/346_1> a\xa0\ncrm:E42_Identifier .',
                'Description': '',
                'Name': 'Move',
                'Identifier': 'LAP.28'
            }
        }
        self.fields1_1 = {
            'rec0wt8UJ7HGXW8WQ': {
                'GroupBy': 'rec2ixJtajtwFemo4',
                'Turtle RDF': '<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;\ncrm:P145i_left_by_by <https://linked.art/example/event/215_1> .\n<https://linked.art/example/event/215_1> a crm:E86_Leaving ;\ncrm:P15_was_influenced_by\n<https://linked.art/example/thing/219_1> .\n<https://linked.art/example/thing/219_1> a\xa0\ncrm:E1_CRM_Entity .',
                'CRM Path': '→P145i→E86[215_1]→P15→E1[219_1]',
                'Description': 'This field is used to link the documented group leaving activity to any entity which had a substantial influence on that activity.',
                'Name': 'Left Group_influence',
                'Identifier': '[LAF.219] Left Group_influence'
            },
            'rec4LWjj03khLbJhQ': {
                'GroupBy': 'rec2ixJtajtwFemo4',
                'Turtle RDF': '<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;\ncrm:P145i_left_by_by <https://linked.art/example/event/215_1> .\n<https://linked.art/example/event/215_1> a crm:E86_Leaving ;\ncrm:P16_used_specific_object\n<https://linked.art/example/physical_object/221_1> .\n<https://linked.art/example/physical_object/221_1> a\xa0\ncrm:E22_Man_Made_Object .',
                'CRM Path': '→P145i→E86[215_1]→P16→E22[221_1]',
                'Description': 'This field is used to link the documented group leaving activity to an instance of physical object used in the exercise of this activity in a manner consequential to its outcome.',
                'Name': 'Left Group_used_Object',
                'Identifier': '[LAF.221] Left Group_used_Object'
            },
            'rec67zf2y03PXYLVA': {
                'GroupBy': 'rec2ixJtajtwFemo4',
                'Turtle RDF': '<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;\ncrm:P145i_left_by_by <https://linked.art/example/event/215_1> .\n<https://linked.art/example/event/215_1> a crm:E86_Leaving ;\ncrm:P117_occurs_during\n<https://linked.art/example/event/216_1> .\n<https://linked.art/example/event/216_1> a\xa0\ncrm:E4_Period .',
                'CRM Path': '→P145i→E86[215_1]→P117→E4[216_1]',
                'Description': 'This field is used to link the documented group leaving activity to a location at which it was carried out.',
                'Name': 'Left Group_during',
                'Identifier': '[LAF.216] Left Group_during'
            },
            'rec2DYQ2cBpejyr0K': {
                'GroupBy': 'rec3ShvB8hEvXrVog',
                'Turtle RDF': '<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P1_is_identified_by\n<https://linked.art/example/identifier/346_1> .\n<https://linked.art/example/identifier/346_1> a\xa0\ncrm:E42_Identifier .',
                'CRM Path': '→P9→E9[333_1]→P1→E42[346_1]',
                'Description': 'This field is used to link the documented move to an identifier that has been attributed to it.',
                'Name': 'Move_identifier',
                'Identifier': '[LAF.346] Move_identifier'
            },
            'rec8BTiY5LWA9batD': {
                'GroupBy': 'rec3ShvB8hEvXrVog',
                'Turtle RDF': '<https://linked.art/example/event/E4> a <http://www.cidoc-crm.org/cidoc-crm/E4_Period>;\ncrm:P9_consists_of <https://linked.art/example/event/333_1> .\n<https://linked.art/example/event/333_1> a crm:E9_Move ;\ncrm:P67i_is_referred_to_by\n<https://linked.art/example/conceptual_object/336_1> .\n<https://linked.art/example/conceptual_object/336_1> a\xa0\ncrm:E33_Linguistic_Object .',
                'CRM Path': '→P9→E9[333_1]→P67i→E33[336_1]',
                'Description': 'This field is used to link the documented move to a textual source which documents the activity itself.',
                'Name': 'Move_source',
                'Identifier': '[LAF.336] Move_source'
            }
        }
        
        self.pattern1.addGroup(self.group1_1)
        self.pattern1.addData(self.fields1_1)


    
    def test_PatternObject(self):
        self.assertTrue(isinstance(self.pattern1, PatternObject))
        self.assertTrue(isinstance(self.pattern1.patternItems, dict))
        grp = next(iter(self.pattern1.patternItems.values()))
        self.assertTrue(isinstance(grp, PatternItem))
        fld = next(iter(grp.GroupedData().values()))
        self.assertTrue(isinstance(fld, dict))
        print(self.pattern1)
        self.assertEqual(str(self.pattern1), "<Pattern> \"Collections\" (2:5)")
        
        
        
if __name__ == '__main__':
    unittest.main()