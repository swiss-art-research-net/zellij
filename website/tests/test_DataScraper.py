import unittest
import MySQLdb  # from pip install mysqlclient
from werkzeug.security import generate_password_hash

from website.db import encrypt, decrypt
from website.DataScraper import DataScraper


class TestDataScraperCRUD(unittest.TestCase):

    TEST_DATABASE = "__test_Scrapers"
    FAKE_SECRET_KEY = b"12345678901234567890123456789012"

    def setUp(self):
        dbnom = TestDataScraperCRUD.TEST_DATABASE
        exe = f"""
            DROP DATABASE IF EXISTS {dbnom};
            CREATE DATABASE IF NOT EXISTS {dbnom}
                CHARACTER SET UTF8
                COLLATE utf8_general_ci;
            USE {dbnom};

            DROP TABLE IF EXISTS Users;
            DROP TABLE IF EXISTS AirTableAccounts;
            DROP TABLE IF EXISTS Scrapers;
            DROP TABLE IF EXISTS ScraperFields;

            CREATE TABLE Users (
                userid INTEGER PRIMARY KEY AUTO_INCREMENT,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                CONSTRAINT UNIQUE ( username(24) )
            );

            CREATE TABLE AirTableAccounts (
                accountid INTEGER PRIMARY KEY AUTO_INCREMENT,
                userkey INTEGER NOT NULL DEFAULT -1,
                accountname TEXT NOT NULL,
                secrettoken BLOB NOT NULL,
                INDEX usersort ( userkey, accountname(24) ),
                CONSTRAINT UNIQUE ( accountname(24) )
            );

            CREATE TABLE AirTableDatabases (
                dbaseid INTEGER PRIMARY KEY AUTO_INCREMENT,
                airtableaccountkey INTEGER,
                dbasename TEXT NOT NULL,
                dbaseapikey CHAR(24) NOT NULL,
                INDEX airtablesort ( airtableaccountkey, dbasename(24) ),
                CONSTRAINT UNIQUE ( airtableaccountkey, dbasename(24) ),
                CONSTRAINT UNIQUE ( dbaseapikey )
            );

            CREATE TABLE Scrapers (
                scraperid INTEGER PRIMARY KEY AUTO_INCREMENT,
                dbasekey INTEGER,
                scrapername TEXT NOT NULL,
                data_table CHAR(32) NOT NULL,
                data_groupby CHAR(32),
                group_table CHAR(32),
                INDEX airtablesort ( dbasekey, scrapername(24) )
            );

            CREATE TABLE ScraperFields (
                scraperfieldid INTEGER PRIMARY KEY AUTO_INCREMENT,
                scraperkey INTEGER,
                sortorder INTEGER,
                tablename TEXT,
                fieldlabel TEXT,
                fieldname TEXT
            );
            """
        self.tables = ["Users", "AirTableAccounts", "Scrapers", "ScraperFields"]
        # print("[DB:Create", end='', flush=True)
        self.testdb = MySQLdb.connect(user="root", passwd="zellij")
        c = self.testdb.cursor()
        c.execute(exe)
        # print(". Insert", end='', flush=True)
        self.passhash1 = generate_password_hash("test password 1")
        self.passhash2 = generate_password_hash("test password 2")
        self.secret1 = encrypt(
            "not a CRUD secret key 1", key=TestDataScraperCRUD.FAKE_SECRET_KEY
        )
        self.secret2 = encrypt(
            "not a CRUD secret key 2", key=TestDataScraperCRUD.FAKE_SECRET_KEY
        )
        self.sampleairtableapikey1 = "appXap4TfbrRkcRSH"
        exe = f"""
        INSERT INTO Users (username, password) VALUES ("testuser1", "{self.passhash1}");
        INSERT INTO Users (username, password) VALUES ("testuser2", "{self.passhash2}");
        INSERT INTO AirTableAccounts (userkey, accountname, secrettoken) VALUES (1, "Test Account 1", %s);
        INSERT INTO AirTableAccounts (userkey, accountname, secrettoken) VALUES (1, "Test Account 2", %s);
        INSERT INTO AirTableDatabases (airtableaccountkey, dbasename, dbaseapikey) VALUES (1, "Sample AirTable", "{self.sampleairtableapikey1}");
        INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_groupby, group_table) VALUES(1, "Fields", "Field", "CRM Class", "CRM Class");
        INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_groupby, group_table) VALUES(1, "Collections", "Collection_fields", "Collection", "Collection");
        INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_groupby, group_table) VALUES(1, "Model", "Model_fields", "Model", "Model");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 1, "Field", "Identifier", "Weight");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 2, "Field", "Name", "Suggested Name");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 3, "Field", "Description", "Description");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 4, "Field", "CRM Path", "CRM Path");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 5, "Field", "Turtle RDF", "Total Turtle");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 6, "CRM Class", "Identifier", "Class_Nim");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 7, "CRM Class", "Name", "ID");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 8, "CRM Class", "Turtle RDF", "Class_Ur_Instance_Turtle");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 1, "Collection_fields", "Identifier", "ID (from Field)");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 2, "Collection_fields", "Name", "Element name (from Field)");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 3, "Collection_fields", "Description", "Description");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 4, "Collection_fields", "CRM Path", "CRM Path");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 5, "Collection_fields", "Turtle RDF", "Total_Turtle");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 6, "Collection", "Identifier", "Identifier");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 7, "Collection", "Name", "Name");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 8, "Collection", "Description", "Description");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 9, "Collection", "Turtle RDF", "Collection_Turtle");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 1, "Model_fields", "Identifier", "Name");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 2, "Model_fields", "Name", "Field Name");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 3, "Model_fields", "Description", "Description");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 4, "Model_fields", "CRM Path", "CRM Path");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 5, "Model_fields", "Turtle RDF", "Model_Fields_Total_Turtle");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 6, "Model", "Identifier", "Identifier");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 7, "Model", "Name", "Name");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 8, "Model", "Description", "Description");
        INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 9, "Model", "Turtle RDF", "Model_Turtle_Prefix");
        """
        c.execute(
            exe,
            (
                self.secret1,
                self.secret2,
            ),
        )
        # print( ".]", end='', flush=True )
        c.close()
        # print( "[Live:", end='', flush=True )
        self.livedb = MySQLdb.connect(user="root", passwd="zellij", db="zellij$website")
        # print("done]", end='', flush=True )

    def tearDown(self):
        dbnom = TestDataScraperCRUD.TEST_DATABASE
        exe = f"DROP DATABASE {dbnom};"
        # print("\n[Test DB: Drop...", end='', flush=True)
        c = self.testdb.cursor()
        c.execute(exe)
        c.close()
        # print( "done]" )

    def test_loadDataScraper(self):
        ds = DataScraper.load("appXap4TfbrRkcRSH", 3, db=self.testdb)
        self.assertTrue(isinstance(ds, DataScraper))
        jsonstruct = {
            "Model": {
                "Model_fields": {
                    "GroupBy": "Model",
                    "Identifier": "Name",
                    "Name": "Field Name",
                    "Description": "Description",
                    "CRM Path": "CRM Path",
                    "Turtle RDF": "Model_Fields_Total_Turtle",
                },
                "Model": {
                    "Identifier": "Identifier",
                    "Name": "Name",
                    "Description": "Description",
                    "Turtle RDF": "Model_Turtle_Prefix",
                },
            }
        }

        self.assertEqual(jsonstruct, ds.dict())

    def test_newDataScraper(self):
        ds = DataScraper.new(self.sampleairtableapikey1, 1, db=self.testdb)
        self.assertTrue(isinstance(ds, DataScraper))

        self.assertEqual(
            decrypt(ds.encryptedtoken, key=TestDataScraperCRUD.FAKE_SECRET_KEY),
            "not a CRUD secret key 1",
        )
        self.assertEqual(ds.name, "")
        self.assertEqual(ds.apikey, self.sampleairtableapikey1)
        self.assertIsNone(ds.dbid)


if __name__ == "__main__":
    unittest.main()
