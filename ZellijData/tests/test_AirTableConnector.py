import unittest
import os.path
import requests

from ZellijData.AirTableConnection import AirTableConnection


class TestAirTableConnection(unittest.TestCase):
    def setUp(self):
        self.schema1 = {
            "Collections": {
                "Collection_fields": {
                    "GroupBy": "Collection",
                    "Turtle RDF": "Total_Turtle",
                    "CRM Path": "CRM Path",
                    "Description": "Description",
                    "Name": "Element name (from Field)",
                    "Identifier": "ID (from Field)",
                },
                "Collection": {
                    "Turtle RDF": "Collection_Turtle",
                    "Description": "Description",
                    "Name": "Name",
                    "Identifier": "Identifier",
                },
            },
            "Fields": {
                "Field": {
                    "GroupBy": "CRM Class",
                    "Turtle RDF": "Total Turtle",
                    "CRM Path": "CRM Path",
                    "Description": "Description",
                    "Name": "Suggested Name",
                    "Identifier": "Identifier",
                },
            },
            "Model": {
                "Model_fields": {
                    "GroupBy": "Model",
                    "Turtle RDF": "Model_Fields_Total_Turtle",
                    "CRM Path": "CRM Path",
                    "Description": "Description",
                    "Name": "Field Name",
                    "Identifier": "Name",
                },
                "Model": {
                    "Turtle RDF": "Model_Turtle_Prefix",
                    "Description": "Description",
                    "Name": "Name",
                    "Identifier": "Identifier",
                },
            },
        }
        self.airtableAPI = "appXap4TfbrRkcRSH"
        # Yes, this is insecure!!! Just testing.
        self.bearerToken = "keyez539eiGXV36lc"

    @unittest.skip("not right now")
    def test_UrlEncoding(self):
        obj = AirTableConnection(self.bearerToken, self.airtableAPI)
        print("About to pull data from an AirTable...")
        testurl = "https://api.airtable.com/v0/appXap4TfbrRkcRSH/Field?fields%5B%5D=CRM+Class&fields%5B%5D=Total+Turtle&fields%5B%5D=CRM+Path&fields%5B%5D=Description&fields%5B%5D=Suggested+Name&fields%5B%5D=Identifer"
        self.assertEqual(
            testurl,
            obj.getUrl(
                "Field",
                [
                    "CRM Class",
                    "Total Turtle",
                    "CRM Path",
                    "Description",
                    "Suggested Name",
                    "Identifer",
                ],
                None,
                None,
            ),
        )

    def test_getSingleGroupedItem(self):
        """
        EXAMPLE FIELD LIST DATA:
        """
        sample_model_schema = {
            "Model_fields": {
                "GroupBy": "Model",
                "Turtle RDF": "Model_Fields_Total_Turtle",
                "CRM Path": "CRM Path",
                "Description": "Description",
                "Name": "Field Name",
                "Identifier": "Name",
            },
            "Model": {
                "Turtle RDF": "Model_Turtle_Prefix",
                "Description": "Description",
                "Name": "Name",
                "Identifier": "Identifier",
            },
        }
        obj = AirTableConnection(self.bearerToken, self.airtableAPI)
        print("About to pull data from an AirTable...")
        x = obj.getSingleGroupedItem("[LAM.9] Person", sample_model_schema)
        # x = obj.getSingleGroupedItem(sample_schema, key="Name", val="[LAM.13] Textual Work")
        # x = obj.getSingleGroupedItem("Model_fields", {'GroupBy': 'Model', 'Turtle RDF': 'Model_Fields_Total_Turtle', 'CRM Path': 'CRM Path', 'Description': 'Description', 'Name': 'Field Name', 'Identifier': 'Name'}, key=None, val=None)
        self.assertEqual(x.Name, "Person")
        self.assertEqual(len(x), 82)
        for i in x.GroupedData().values():
            print(i)

    def test_getListOfGroups(self):
        """
        EXAMPLE FIELD LIST DATA:
        """
        sample_model_schema = {
            "Model_fields": {
                "GroupBy": "Model",
                "Turtle RDF": "Model_Fields_Total_Turtle",
                "CRM Path": "CRM Path",
                "Description": "Description",
                "Name": "Field Name",
                "Identifier": "Name",
            },
            "Model": {
                "Turtle RDF": "Model_Turtle_Prefix",
                "Description": "Description",
                "Name": "Name",
                "Identifier": "Identifier",
            },
        }
        obj = AirTableConnection(self.bearerToken, self.airtableAPI)
        print("About to pull data from an AirTable...")
        x = obj.getListOfGroups(sample_model_schema)
        self.assertEqual(len(x), 16)


if __name__ == "__main__":
    unittest.main()
