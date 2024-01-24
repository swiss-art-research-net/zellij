import unittest
import os.path
import requests

from ZellijTable.AggregateDataCollector import AggregateDataCollector
from ZellijTable.AirTableConnection import EnhancedResponse

class TestDataCollectionFromAirTable(unittest.TestCase):
    
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
                    'Identifier': 'Identifer'
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
        self.bearerToken = "keyez539eiGXV36lc"

    
    def test_DataCollection(self):
        obj = AggregateDataCollector(self.schema1)
        print("About to pull data from an AirTable...")
        r = obj.get(self.bearerToken, self.airtableAPI)
        print(".get() finished.")
        
        print(r)
        if isinstance(r, EnhancedResponse):
            print(r.response.text)
        
        
if __name__ == '__main__':
    unittest.main()