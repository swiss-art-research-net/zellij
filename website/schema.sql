DROP DATABASE IF EXISTS zellij$website;
CREATE DATABASE IF NOT EXISTS zellij$website
	CHARACTER SET UTF8
    COLLATE utf8_general_ci;
USE zellij$website;

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

INSERT INTO Users (username, password) VALUES("stilpo", "pbkdf2:sha256:150000$Eq7HTA4w$d1b701d4e7fd1181c75b6177d2ae5751d5e95b01e4278c1843a0aabedd21e2d8");

CREATE TABLE AirTableAccounts (
	accountid INTEGER PRIMARY KEY AUTO_INCREMENT,
	userkey INTEGER NOT NULL DEFAULT -1,
	accountname TEXT NOT NULL,
	secrettoken BLOB NOT NULL,
	
	INDEX usersort ( userkey, accountname(24) ),
	
	CONSTRAINT UNIQUE ( accountname(24) )
);

INSERT INTO AirTableAccounts (userkey, accountname, secrettoken) VALUES(1, "Sample: AirTable account", "not so secret key");
INSERT INTO AirTableAccounts (userkey, accountname, secrettoken) VALUES(2, "Somebody else's account", "not so secret key");

CREATE TABLE AirTableDatabases (
	dbaseid INTEGER PRIMARY KEY AUTO_INCREMENT,
	airtableaccountkey INTEGER,
	dbasename TEXT NOT NULL,
	dbaseapikey CHAR(24) NOT NULL,
    
    INDEX airtablesort ( airtableaccountkey, dbasename(24) ),
    
	CONSTRAINT UNIQUE ( airtableaccountkey, dbasename(24) ),
	CONSTRAINT UNIQUE ( dbaseapikey )
);

INSERT INTO AirTableDatabases (airtableaccountkey, dbasename, dbaseapikey) VALUES(1, "Sample: Copy of Getty Digital", "app2n9fqx4VVNF4mM");

CREATE TABLE Scrapers (
	scraperid INTEGER PRIMARY KEY AUTO_INCREMENT,
	dbasekey INTEGER,
	scrapername TEXT NOT NULL,
	data_table CHAR(32) NOT NULL,
    data_keyfield CHAR(32) NOT NULL,
	data_groupby CHAR(32),
	group_table CHAR(32),
    group_keyfield CHAR(32),
    
    INDEX airtablesort ( dbasekey, scrapername(24) )
);

INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_keyfield, data_groupby, group_table, group_keyfield) VALUES(1, "Fields", "Field", "ID", "CRM Class", "CRM Class", "ID");
INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_keyfield, data_groupby, group_table, group_keyfield) VALUES(1, "Collections", "Collection_Fields", "Name", "Collection", "Collection", "ID");
INSERT INTO Scrapers (dbasekey, scrapername, data_table, data_keyfield, data_groupby, group_table, group_keyfield) VALUES(1, "Model", "Model_Fields", "Name", "Model", "Model", "ID");

CREATE TABLE ScraperFields (
	scraperfieldid INTEGER PRIMARY KEY AUTO_INCREMENT,
	scraperkey INTEGER,
	sortorder INTEGER,
	tablename TEXT,
	fieldlabel TEXT,
	fieldname TEXT,
    sortable BOOLEAN,
    groupable BOOLEAN
);

INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname, sortable, groupable) VALUES(1, 1, "Field", "Weight", "Weight", 1, 0);
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname, sortable, groupable) VALUES(1, 2, "Field", "Group", "Ontology Modeller Field Name", 0, 1);
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 3, "Field", "Identifier", "Weight");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 4, "Field", "Name", "Suggested Name");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 5, "Field", "Description", "Description");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 6, "Field", "CRM Path", "CRM Path");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 7, "Field", "Turtle RDF", "Total Turtle");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 8, "CRM Class", "Identifier", "Class_Nim");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 9, "CRM Class", "Name", "ID");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(1, 10, "CRM Class", "Turtle RDF", "Class_Ur_Instance_Turtle");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 1, "Collection_Fields", "Identifier", "ID (from Field)");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 2, "Collection_Fields", "Name", "Element name (from Field)");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 3, "Collection_Fields", "Description", "Description");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 4, "Collection_Fields", "CRM Path", "CRM Path");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 5, "Collection_Fields", "Turtle RDF", "Total_Turtle");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 6, "Collection", "Identifier", "Identifier");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 7, "Collection", "Name", "Name");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 8, "Collection", "Description", "Description");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(2, 9, "Collection", "Turtle RDF", "Collection_Turtle");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 1, "Model_Fields", "Identifier", "Name");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 2, "Model_Fields", "Name", "Field Name");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 3, "Model_Fields", "Description", "Description");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 4, "Model_Fields", "CRM Path", "CRM Path");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 5, "Model_Fields", "Turtle RDF", "Model_Fields_Total_Turtle");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 6, "Model", "Identifier", "Identifier");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 7, "Model", "Name", "Name");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 8, "Model", "Description", "Description");
INSERT INTO ScraperFields (scraperkey, sortorder, tablename, fieldlabel, fieldname) VALUES(3, 9, "Model", "Turtle RDF", "Model_Turtle_Prefix");


/*
CREATE TABLE Scrapers (
	scraperid INTEGER PRIMARY KEY AUTO_INCREMENT,
	dbasekey INTEGER,
	scrapername TEXT NOT NULL,
	data_table CHAR(32) NOT NULL,
	data_uniqueid CHAR(32),
	data_groupby CHAR(32),
	data_identifier CHAR(32),
	data_name CHAR(32),
	data_fullname CHAR(32),
	data_description CHAR(32),
	data_crmpath CHAR(32),
	data_turtle CHAR(32),
	group_table CHAR(32),
	group_uniqueid CHAR(32),
	group_identifier CHAR(32),
	group_name CHAR(32),
	group_fullname CHAR(32),
	group_description CHAR(32),
	group_turtle CHAR(32),
    
    INDEX airtablesort ( dbasekey, scrapername(24) ),
    
	CONSTRAINT UNIQUE ( scrapername(24) )
);

INSERT INTO Scrapers (
	dbasekey, scrapername,
	data_table, data_uniqueid, data_groupby, data_identifier, data_name, data_fullname, data_description, data_crmpath, data_turtle
	) VALUES(
	1, "Fields",
	"Fields", "", "CRM Class", "Identifier", "Suggested Name", "ID", "Description", "CRM Path", "Total Turtle"
);
INSERT INTO Scrapers (
	dbasekey, scrapername,
	data_table, data_uniqueid, data_groupby, data_identifier, data_name, data_fullname, data_description, data_crmpath, data_turtle,
	group_table, group_uniqueid, group_identifier, group_name, group_fullname, group_description, group_turtle
	) VALUES(
	1, "Collections",
	"Collection_fields", "" ,"Collection", "ID (from Field)", "Element name (from Field)", "Name", "Description", "CRM Path", "Total_Turtle",
	"Collection", "", "Identifier", "Name", "ID", "Description", "Collection_Turtle"
);
INSERT INTO Scrapers (
	dbasekey, scrapername,
	data_table, data_uniqueid, data_groupby, data_identifier, data_name, data_fullname, data_description, data_crmpath, data_turtle,
	group_table, group_uniqueid, group_identifier, group_name, group_fullname, group_description, group_turtle
	) VALUES(
	1, "Model",
	"Model_fields", "" ,"Model", "Field Name", "Field Name", "Name", "Description", "CRM Path", "Model_Fields_Total_Turtle",
	"Model", "", "Identifier", "Name", "ID", "Description", "Model_Turtle_Prefix"
);
/*