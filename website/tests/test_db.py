import unittest
import nacl
import os.path

import MySQLdb  # from pip install mysqlclient

from website.db import (
    get_airtable_pattern,
    new_airtable_pattern,
    set_airtable_pattern,
    get_airtable_pattern_by_name,
)
from website.db import generate_airtable_schema, decrypt, encrypt
from website.DataScraper import DataScraper
from werkzeug.security import check_password_hash, generate_password_hash


class TestNaClSymmetricEncryption(unittest.TestCase):
    def setUp(self):
        self.SYMMETRIC_KEYFILE = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..",
            "..",
            "..",
            "ZellijSecrets",
            "secretkeyfile.bytes",
        )

    def test_findSecretKeyFile(self):
        with open(self.SYMMETRIC_KEYFILE, "rb") as f:
            secretkey = f.read()

        self.assertEqual(len(secretkey), nacl.secret.SecretBox.KEY_SIZE)

    def test_randomNonces(self):
        text = "No one expects the Spanish Inquisition!"
        xxx = encrypt(text, keyfile=self.SYMMETRIC_KEYFILE)
        yyy = encrypt(text, keyfile=self.SYMMETRIC_KEYFILE)

        self.assertNotEqual(xxx, yyy)

    def test_roundtrip(self):
        text = "No one expects the Spanish Inquisition!"
        xxx = encrypt(text, keyfile=self.SYMMETRIC_KEYFILE)
        cleartext = decrypt(xxx, keyfile=self.SYMMETRIC_KEYFILE)

        self.assertEqual(text, cleartext)


class TestScraperFieldDataManagement(unittest.TestCase):
    TEST_DATABASE = "__test_Scrapers"
    FAKE_SECRET_KEY = b"12345678901234567890123456789012"

    def setUp(self):
        dbnom = TestScraperFieldDataManagement.TEST_DATABASE
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
        print("[DB:Create", end="", flush=True)
        self.testdb = MySQLdb.connect(user="root", passwd="zellij")
        c = self.testdb.cursor()
        c.execute(exe)
        print(". Insert", end="", flush=True)
        self.passhash1 = generate_password_hash("test password 1")
        self.passhash2 = generate_password_hash("test password 2")
        self.secret1 = encrypt(
            "not a secret key 1", key=TestScraperFieldDataManagement.FAKE_SECRET_KEY
        )
        self.secret2 = encrypt(
            "not a secret key 2", key=TestScraperFieldDataManagement.FAKE_SECRET_KEY
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
        print(".]", end="", flush=True)
        c.close()
        # print( "[Live:", end='', flush=True )
        self.livedb = MySQLdb.connect(user="root", passwd="zellij", db="zellij$website")
        # print("done]", end='', flush=True )

    def tearDown(self):
        dbnom = TestScraperFieldDataManagement.TEST_DATABASE
        exe = f"DROP DATABASE {dbnom};"
        # print("\n[Test DB: Drop...", end='', flush=True)
        c = self.testdb.cursor()
        c.execute(exe)
        c.close()
        # print( "done]" )

    @unittest.skip("preserving database")
    def test_compareSchemas(self):
        clive = self.livedb.cursor()
        ctest = self.testdb.cursor()
        clive.execute("SHOW TABLES")
        livelist = [x[0] for x in clive.fetchall()]
        ctest.execute("SHOW TABLES")
        testlist = [x[0] for x in ctest.fetchall()]
        self.assertEqual(livelist, testlist)

        for tbl in livelist:
            clive.execute(f"SHOW COLUMNS FROM {tbl}")
            livecols = clive.fetchall()
            ctest.execute(f"SHOW COLUMNS FROM {tbl}")
            testcols = ctest.fetchall()
            self.assertEqual(livecols, testcols)

    def test_ScraperHandling(self):
        self.maxDiff = None
        # test read
        tmpschema1 = {
            "Collections": {
                "Collection": {
                    "Identifier": "Identifier",
                    "Name": "Name",
                    "Description": "Description",
                    "Turtle RDF": "Collection_Turtle",
                },
                "Collection_fields": {
                    "Identifier": "ID (from Field)",
                    "Name": "Element name (from Field)",
                    "Description": "Description",
                    "CRM Path": "CRM Path",
                    "Turtle RDF": "Total_Turtle",
                    "GroupBy": "Collection",
                },
            },
            "Fields": {
                "CRM Class": {
                    "Identifier": "Class_Nim",
                    "Name": "ID",
                    "Turtle RDF": "Class_Ur_Instance_Turtle",
                },
                "Field": {
                    "Identifier": "Weight",
                    "Name": "Suggested Name",
                    "Description": "Description",
                    "CRM Path": "CRM Path",
                    "Turtle RDF": "Total Turtle",
                    "GroupBy": "CRM Class",
                },
            },
            "Model": {
                "Model": {
                    "Identifier": "Identifier",
                    "Name": "Name",
                    "Description": "Description",
                    "Turtle RDF": "Model_Turtle_Prefix",
                },
                "Model_fields": {
                    "Identifier": "Name",
                    "Name": "Field Name",
                    "Description": "Description",
                    "CRM Path": "CRM Path",
                    "Turtle RDF": "Model_Fields_Total_Turtle",
                    "GroupBy": "Model",
                },
            },
        }
        tmpschema2, tmpsecret = generate_airtable_schema(
            self.sampleairtableapikey1, db=self.testdb
        )
        self.assertDictEqual(tmpschema1, tmpschema2)
        self.assertTrue(isinstance(tmpsecret, bytes))

        pattern1 = DataScraper(
            self.sampleairtableapikey1,
            "Model",
            "Model_fields",
            "Model",
            "Model",
            tabledata={
                "Identifier": "Name",
                "Name": "Field Name",
                "Description": "Description",
                "CRM Path": "CRM Path",
                "Turtle RDF": "Model_Fields_Total_Turtle",
            },
            groupdata={
                "Identifier": "Identifier",
                "Name": "Name",
                "Description": "Description",
                "Turtle RDF": "Model_Turtle_Prefix",
            },
        )
        pattern2 = get_airtable_pattern(self.sampleairtableapikey1, 3, db=self.testdb)
        self.assertDictEqual(pattern1.dict(), pattern2.dict())

        data1 = DataScraper(
            self.sampleairtableapikey1,
            "Model Changed",
            "Model_fields4",
            "Model3",
            "Model3",
            dbid=3,
            tabledata={
                "Identifier": "ID",
                "Name": "Name1",
                "Description": "Description",
                "CRM Path": "CRM Path",
                "Turtle RDF": "Model_Fields_Total_Turtle",
                "New Field 1": "Anything goes",
            },
            groupdata={
                "Identifier": "Ident",
                "Name": "Name2",
                "Description": "Description",
            },
        )
        o = set_airtable_pattern(data1, db=self.testdb, forcepermission=True)
        self.assertTrue(o)

        # pattern3 is a duplicate of data1; making sure there's no issue between creation and use
        pattern3 = DataScraper(
            self.sampleairtableapikey1,
            "Model Changed",
            "Model_fields4",
            "Model3",
            "Model3",
            tabledata={
                "Identifier": "ID",
                "Name": "Name1",
                "Description": "Description",
                "CRM Path": "CRM Path",
                "Turtle RDF": "Model_Fields_Total_Turtle",
                "New Field 1": "Anything goes",
            },
            groupdata={
                "Identifier": "Ident",
                "Name": "Name2",
                "Description": "Description",
            },
        )
        pattern4 = get_airtable_pattern(self.sampleairtableapikey1, 3, db=self.testdb)
        self.assertDictEqual(pattern3.dict(), pattern4.dict())

        """
        data = {
            "scrapername": "New Scraper",
            "data_table": "dt",
            "data_groupby": "thing",
            "group_table": "thing",
            "Data": {
                "Snark": "Boojum",
                "Bandersnatch": "Calloo, callay"
            },
            "Group": {
                "Required": "Nothing, obviously"
            }
        }
        o = set_airtable_pattern(None, data, db=self.testdb, forcepermission=True, apikey=self.sampleairtableapikey1)
        pattern5 = get_airtable_pattern(self.sampleairtableapikey1, None, db=self.testdb)
        #print(pattern5)
        """

    def test_NewScraperHandling(self):
        self.maxDiff = None
        obj = new_airtable_pattern(self.sampleairtableapikey1, 1, db=self.testdb)
        self.assertEqual(
            decrypt(
                obj.encryptedtoken, key=TestScraperFieldDataManagement.FAKE_SECRET_KEY
            ),
            "not a secret key 1",
        )
        self.assertEqual(obj.name, "")
        self.assertEqual(obj.apikey, self.sampleairtableapikey1)
        self.assertIsNone(obj.dbid)

    def test_ScraperByName(self):
        obj = get_airtable_pattern_by_name(
            self.sampleairtableapikey1, "Model", db=self.testdb
        )
        print(obj)
        self.assertTrue(isinstance(obj, DataScraper))


if __name__ == "__main__":
    unittest.main()
