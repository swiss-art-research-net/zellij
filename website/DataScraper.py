"""
Created on Mar. 23, 2021

@author: Pete Harris
"""

from collections import OrderedDict


class DataScraper(object):
    """
    classdocs
    """

    @staticmethod
    def load(apikey, scraperid, db=None, validateuserid=None):
        """
        Calls the loading function for scrapers from the module "db".
        That code belongs in the DB-specific module, in case the storage method changes.
        """
        import website.db

        return website.db.get_airtable_pattern(apikey, scraperid, db, validateuserid)

    @staticmethod
    def new(apikey, validateuserid, db=None):
        """
        Calls the new function for scrapers from the module "db".
        That code belongs in the DB-specific module, in case the storage method changes.
        """
        import website.db

        return website.db.new_airtable_pattern(apikey, validateuserid, db)

    def __init__(
        self,
        apikey,
        name,
        tablename,
        tablekeyfield,
        groupby,
        groupname,
        groupkeyfield,
        groupsorttable,
        groupsortcolumn,
        groupsortname,
        tabledata={},
        groupdata={},
        encryptedtoken=None,
        dbid=None,
    ):
        """
        tabledata and groupdata both are a key/value pair list where the value consists of the following dictionary triple:
        { "name": <string>, "sortable": <boolean>, "groupable": <boolean> }
        """
        self.apikey = apikey
        self.name = name
        self.data_table = tablename
        self.data_keyfield = tablekeyfield
        self.data_groupby = groupby
        self.group_table = groupname
        self.group_keyfield = groupkeyfield
        self.group_sorttable = groupsorttable
        self.group_sortcolumn = groupsortcolumn
        self.group_sortname = groupsortname
        self.Data = OrderedDict(tabledata)
        self.Group = OrderedDict(groupdata)
        self.encryptedtoken = encryptedtoken
        self.dbid = dbid

    def __str__(self, multiline=False):
        first = "<DataScraper:" + (f'"{self.name}"' if self.name else "") + ">"
        second = (
            self.data_table
            + "["
            + ", ".join(
                [
                    f"""'{x}':'{y["name"]}'"""
                    + ("↓" if y["sortable"] else "")
                    + ("◊" if y["groupable"] else "")
                    + ("-" if y["hideable"] else "")
                    for x, y in self.Data.items()
                ]
            )
            + "]"
        )
        third = (
            self.group_table
            + "["
            + ", ".join(
                [
                    f"""'{x}':'{y["name"]}'"""
                    + ("↓" if y["sortable"] else "")
                    + ("◊" if y["groupable"] else "")
                    + ("-" if y["hideable"] else "")
                    for x, y in self.Group.items()
                ]
            )
            + "]"
        )

        if multiline:
            return "\n".join(
                [
                    first,
                    "    " + second,
                    ("  → " if self.data_groupby else "    ") + third,
                ]
            )
        else:
            return (
                first
                + " ("
                + second
                + (" → " if self.data_groupby else " ")
                + third
                + ")"
            )

    def addDataItem(
        self,
        key,
        val,
        sortable=False,
        groupable=False,
        hideable=False,
        function=None,
        link=None,
    ):
        self.Data[key] = {
            "name": val,
            "sortable": sortable,
            "groupable": groupable,
            "hideable": hideable,
            "function": function,
            "link": link,
        }

    def addGroupItem(
        self,
        key,
        val,
        sortable=False,
        groupable=False,
        hideable=False,
        function=None,
        link=None,
    ):
        self.Group[key] = {
            "name": val,
            "sortable": sortable,
            "groupable": groupable,
            "hideable": hideable,
            "function": function,
            "link": link,
        }

    def dict(self):
        d = {self.name: {}}
        d[self.name][self.data_table] = {
            "KeyField": self.data_keyfield,
            "GroupBy": self.data_groupby,
        }
        d[self.name][self.group_table] = {"KeyField": self.group_keyfield}
        for k, v in self.Data.items():
            d[self.name][self.data_table][k] = v
        for k, v in self.Group.items():
            d[self.name][self.group_table][k] = v
        return d
