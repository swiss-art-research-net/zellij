import unittest
import os.path
import requests
import io
import contextlib

from ZellijTable.RDFCodeBlock import RDFCodeBlock


class TestErrorOut(unittest.TestCase):
    def setUp(self):
        pass

    def test_TurtleSingleBadURL(self):
        txt = """@prefix sari: <http://www.sari.com> .
@prefix crmdig: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix frbr: <http://iflastandards.info/ns/fr/frbr/frbroo/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix la: <http://linked.art> .
@prefix dcterms: <http://decterms> .
@prefix dc: <http://dc> .
@prefix skos: <http://skos> .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P15_was_influenced_by
<https://linked.art/example/thing/219_1> .
<https://linked.art/example/thing/219_1> a
crm:E1_CRM_Entity .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P16_used_specific_object
<https://linked.art/example/physical_object/221_1> .
<https://linked.art/example/physical_object/221_1> a
crm:E22_Man_Made_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P117_occurs_during
<https://linked.art/example/event/216_1> .
<https://linked.art/example/event/216_1> a
crm:E4_Period .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/227 _1> .
<https://linked.art/example/conceptual_object/227 _1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P4_has_time-span
<https://linked.art/example/time_span/222_1> .
<https://linked.art/example/time_span/222_1> a
crm:E52_Time-Span .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P1_is_identified_by
<https://linked.art/example/identifier/226_1> .
<https://linked.art/example/identifier/226_1> a
crm:E42_Identifier .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P144_joined_with <https://linked.art/example/actor/228_1> .
<https://linked.art/example/actor/228_1> a crm:E74_Group .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P14_carried_out_by
<https://linked.art/example/actor/218_1> .
<https://linked.art/example/actor/218_1> a
crm:E39_Actor .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P2_has_type
<https://linked.art/example/type/223_1> .
<https://linked.art/example/type/223_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/217_1> .
<https://linked.art/example/conceptual_object/217_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
rdfs:label "Formation_label_value" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P7_took_place_at
<https://linked.art/example/place/215_2> .
<https://linked.art/example/place/215_2> a
crm:E53_Place .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P32_used_general_technique
<https://linked.art/example/type/225_1> .
<https://linked.art/example/type/225_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;
crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P1_is_identified_by
<https://linked.art/example/name/220_1> .
<https://linked.art/example/name/220_1> a
crm:E33_E41_Linguistic_Appellation .
"""

        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            obj = RDFCodeBlock(txt)
        self.assertIn(
            "https://linked.art/example/conceptual_object/227 _1 does not look like a valid URI, trying to serialize this will break.",
            buf.getvalue(),
        )

    def test_TurtleMultipleBadURL(self):
        txt = """@prefix sari: <http://www.sari.com> .
@prefix crmdig: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix frbr: <http://iflastandards.info/ns/fr/frbr/frbroo/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix la: <http://linked.art> .
@prefix dcterms: <http://decterms> .
@prefix dc: <http://dc> .
@prefix skos: <http://skos> .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P4_has_time-span
<https://linked.art/example/time_span/222_1> .
<https://linked.art/example/time_span/222_1> a
crm:E52_Time-Span .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P15_was_influenced_by
<https://linked.art/example/thing/219_1> .
<https://linked.art/example/thing/219_1> a
crm:E1_CRM_Entity .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P4_has_time-span
<https://linked.art/example/time_span/208_1> .
<https://linked.art/example/time_span/208_1> a
crm:E52_Time-Span .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P32_used_general_technique
<https://linked.art/example/type/225_1> .
<https://linked.art/example/type/225_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P14_carried_out_by
<https://linked.art/example/actor/218_1> .
<https://linked.art/example/actor/218_1> a
crm:E39_Actor .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P15_was_influenced_by
<https://linked.art/example/thing/130_1> .
<https://linked.art/example/thing/130_1> a
crm:E1_CRM_Entity .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P16_used_specific_object
<https://linked.art/example/physical_object/221_1> .
<https://linked.art/example/physical_object/221_1> a
crm:E22_Man_Made_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P2_has_type
<https://linked.art/example/type/223_1> .
<https://linked.art/example/type/223_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
rdfs:label "Formation_label_value" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P117_occurs_during
<https://linked.art/example/event/216_1> .
<https://linked.art/example/event/216_1> a
crm:E4_Period .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P144_joined_with <https://linked.art/example/actor/214_1> .
<https://linked.art/example/actor/214_1> a crm:E74_Group .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
rdfs:label "Formation_label_value" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P32_used_general_technique
<https://linked.art/example/type/136_1> .
<https://linked.art/example/type/136_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/168_1> .
<https://linked.art/example/conceptual_object/168_1> a crm:E42_Identifier;
crm:P67i_is_referred_to_by <https://linked.art/example/conceptual_object/171_1> .
<https://linked.art/example/conceptual_object/171_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P1_is_identified_by
<https://linked.art/example/name/220_1> .
<https://linked.art/example/name/220_1> a
crm:E33_E41_Linguistic_Appellation .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P107i_is_current_or_former_member_of <https://linked.art/example/actor/167_1> .
<https://linked.art/example/actor/167_1> a crm:E74_Group .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/227 _1> .
<https://linked.art/example/conceptual_object/227 _1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P1_is_identified_by
<https://linked.art/example/identifier/212_1> .
<https://linked.art/example/identifier/212_1> a
crm:E42_Identifier .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P2_has_type <https://linked.art/example/type/229_1> .
<https://linked.art/example/type/229_1> a crm:E55_Type ;
crm:P2_has_type <https://linked.art/example/type/229 _2> .
<https://linked.art/example/type/229 _2> a crm:E55_type ;
rdfs:label "Nationality" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/138_1> .
<https://linked.art/example/conceptual_object/138_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/128_1> .
<https://linked.art/example/conceptual_object/128_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P74_has_current_or_former_residence <https://linked.art/example/place/172_1> .
<https://linked.art/example/place/172_1> a crm:E53_Place .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P7_took_place_at
<https://linked.art/example/place/215_2> .
<https://linked.art/example/place/215_2> a
crm:E53_Place .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P2_has_type <https://linked.art/example/type/231_1> .
<https://linked.art/example/type/231_1> a crm:E55_Type ;
crm:P2_has_type <https://linked.art/example/type/231 _2> .
<https://linked.art/example/type/231 _2> a crm:E55_type ;
rdfs:label "Ethnicity" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P117_occurs_during
<https://linked.art/example/event/127_1> .
<https://linked.art/example/event/127_1> a
crm:E4_Period .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P7_took_place_at
<https://linked.art/example/place/126_2> .
<https://linked.art/example/place/126_2> a
crm:E53_Place .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/168_1> .
<https://linked.art/example/conceptual_object/168_1> a crm:E42_Identifier;
crm:P190_has_symbolic_content "Identifier_value_content" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/213_1> .
<https://linked.art/example/conceptual_object/213_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P1_is_identified_by
<https://linked.art/example/identifier/226_1> .
<https://linked.art/example/identifier/226_1> a
crm:E42_Identifier .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P117_occurs_during
<https://linked.art/example/event/202_1> .
<https://linked.art/example/event/202_1> a
crm:E4_Period .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P1_is_identified_by
<https://linked.art/example/identifier/137_1> .
<https://linked.art/example/identifier/137_1> a
crm:E42_Identifier .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P16_used_specific_object
<https://linked.art/example/physical_object/207_1> .
<https://linked.art/example/physical_object/207_1> a
crm:E22_Man_Made_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P144_joined_with <https://linked.art/example/actor/228_1> .
<https://linked.art/example/actor/228_1> a crm:E74_Group .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/203_1> .
<https://linked.art/example/conceptual_object/203_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P4_has_time-span
<https://linked.art/example/time_span/133_1> .
<https://linked.art/example/time_span/133_1> a
crm:E52_Time-Span .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P2_has_type
<https://linked.art/example/type/209_1> .
<https://linked.art/example/type/209_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P145i_left_by_by <https://linked.art/example/event/215_1> .
<https://linked.art/example/event/215_1> a crm:E86_Leaving ;
crm:P67i_is_referred_to_by
<https://linked.art/example/conceptual_object/217_1> .
<https://linked.art/example/conceptual_object/217_1> a
crm:E33_Linguistic_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P32_used_general_technique
<https://linked.art/example/type/211_1> .
<https://linked.art/example/type/211_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P15_was_influenced_by
<https://linked.art/example/thing/205_1> .
<https://linked.art/example/thing/205_1> a
crm:E1_CRM_Entity .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P1_is_identified_by
<https://linked.art/example/name/206_1> .
<https://linked.art/example/name/206_1> a
crm:E33_E41_Linguistic_Appellation .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P1_is_identified_by
<https://linked.art/example/name/131_1> .
<https://linked.art/example/name/131_1> a
crm:E33_E41_Linguistic_Appellation .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P14_carried_out_by
<https://linked.art/example/actor/129_1> .
<https://linked.art/example/actor/129_1> a
crm:E39_Actor .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
rdfs:label "Professional Activity_label_value" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P2_has_type
<https://linked.art/example/type/134_1> .
<https://linked.art/example/type/134_1> a
crm:E55_Type .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/168_1> .
<https://linked.art/example/conceptual_object/168_1> a crm:crm:E42_Identifier ;
crm:P2_has_type <https://linked.art/example/identifier/169_1> .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P14i_performed <https://linked.art/example/event/126_1> .
<https://linked.art/example/event/126_1> a crm:E7_Activity ;
crm:P16_used_specific_object
<https://linked.art/example/physical_object/132_1> .
<https://linked.art/example/physical_object/132_1> a
crm:E22_Man_Made_Object .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P1_is_identified_by <https://linked.art/example/conceptual_object/168_1> .
<https://linked.art/example/conceptual_object/168_1> a crm:E42_Identifier;
rdfs:label "Identifier_label_value" .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P14_carried_out_by
<https://linked.art/example/actor/204_1> .
<https://linked.art/example/actor/204_1> a
crm:E39_Actor .
<https://linked.art/example/actor/E39> a <http://www.cidoc-crm.org/cidoc-crm/E39_Actor>;crm:P143i_was_joined_by_by <https://linked.art/example/event/201_1> .
<https://linked.art/example/event/201_1> a crm:E85_Joining ;
crm:P7_took_place_at
<https://linked.art/example/place/201_2> .
<https://linked.art/example/place/201_2> a
crm:E53_Place .
"""

        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            obj = RDFCodeBlock(txt)
        errstr = buf.getvalue()
        self.assertIn(
            "https://linked.art/example/conceptual_object/227 _1 does not look like a valid URI, trying to serialize this will break.",
            errstr,
        )
        self.assertIn(
            "https://linked.art/example/type/229 _2 does not look like a valid URI, trying to serialize this will break.",
            errstr,
        )
        self.assertIn(
            "https://linked.art/example/type/231 _2 does not look like a valid URI, trying to serialize this will break.",
            errstr,
        )
        self.assertTrue(
            "https://linked.art/example/conceptual_object/227 _1" in obj.warnings
        )
        self.assertTrue("https://linked.art/example/type/229 _2" in obj.warnings)
        self.assertTrue("https://linked.art/example/type/231 _2" in obj.warnings)


if __name__ == "__main__":
    unittest.main()
