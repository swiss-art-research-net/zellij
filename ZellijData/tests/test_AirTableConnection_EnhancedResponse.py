import unittest
import os.path
import requests

from ZellijTable.AirTableConnection import AirTableConnection, EnhancedResponse


class TestAirTableConnection_EnhancedResponse(unittest.TestCase):
    
    def setUp(self):
        self.schema1 = {
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
                    'Turtle RDF': 'Collection_Turtle',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            },
            'Fields': {
                'Field': {
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
        self.airtableAPI = "appXap4TfbrRkcRSH"
        # Yes, this is insecure!!! Just testing.
        # TODO: fix this, or at least create a new AirTable user for debugging
        self.bearerToken = "keyez539eiGXV36lc"
        self.badToken =    "slflilkdnflsknfld"
    
    
    def test_BadToken(self):
        obj = AirTableConnection(self.badToken, self.airtableAPI)
        print("About to fail to pull data from an AirTable...")
        result = obj.get("Field", ["CRM Class", "Total Turtle", "CRM Path", "Description", "Suggested Name", "Identifer"])
        print(type(result), result)
        self.assertTrue(isinstance(result, EnhancedResponse))
        
        
        
if __name__ == '__main__':
    unittest.main()