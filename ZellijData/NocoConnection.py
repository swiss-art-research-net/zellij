"""
Created on Mar. 9, 2021

@author: Pete Harris
"""

import requests
import re
from urllib.parse import urlencode, unquote_plus, urlparse
import os
from dotenv import load_dotenv
from ZellijData.SingleGroupedItem import SingleGroupedItem
from ZellijData.TurtleCodeBlock import TurtleCodeBlock
import logging

logging.basicConfig(level=logging.DEBUG)


class NocoConnection(object):
    """
    Note: all this retry and offset looping code has to happen in the other module, where the data can be stored.
    Leaving the notes here for now though.

    Need to be able to do retries, and also to fail after them if need be.

    Currently getting "[503] Service Unavailable: Backend server is at capacity".

    Also possible to get "[429] Too Many Requests" if you hit it more than 5 requests per sec (per base). Must wait 30 seconds after this.
        - Reponse sometimes includes Retry-After: 3600 (for example) to tell you how long to wait.

    If you try to access the AirTables metadata without having that power activated, you get:
        https://api.airtable.com/v0/meta/bases
        404 Not Found
    """

    def __init__(self, dbaseAPI, friendlyname=""):
        """
        Constructor
        """
        load_dotenv()
        self.xcToken = os.getenv("xcToken")
        self.airTableBaseAPI = dbaseAPI
        self.friendlyname = friendlyname
        self.headers = {"xc-token": self.xcToken}

    def get(self):
        out = []

        for patternkey, pattern in self.schema.items():
            p = PatternObject(patternkey)
            low = None
            high = None
            error = None
            for tablename, fieldlist in pattern.items():
                if "GroupBy" in fieldlist:
                    low = self.getsinglecall(tablename, fieldlist)
                else:
                    high = self.getsinglecall(tablename, fieldlist)
            if isinstance(low, EnhancedResponse):
                return low
            if isinstance(high, EnhancedResponse):
                return high
            p.addGroup(high)
            p.addData(low)
            p.generateGraphs()
            out.append(p)
        return out

    def getSingleGroupedItem(self, idsearchterm, schema, maxrecords=None, sort=None):
        """
        Schema is a two-entry dictionary containing tablename:{fields}, that identify the paired Group and Base Data; i.e.
             {
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
        The lower-level data can be identified because it contains a GroupBy field, which is mandatory.
        """
        for tablename, fieldlist in schema.items():
            if "GroupBy" in fieldlist:
                lowremapper = fieldlist
                lowfields = list(fieldlist.values())
                lowtable = tablename
                lowgroupby = fieldlist["GroupBy"]
            else:
                highremapper = fieldlist
                highfields = list(fieldlist.values())
                hightable = tablename

        url = self._getUrl(hightable, highfields, formula=idsearchterm)
        highresponse = requests.get(url, headers=self.headers)
        logging.debug(
            "%s*****getSingleGroupedItem-list-response*******", highresponse.json()
        )
        # parse response here
        if len(highresponse.json()["list"]) == 0:
            return None
        # groupid = highresponse.json()["list"][0]["id"]
        # logging.debug('%s*****groupid*******', groupid)
        searchtext = highresponse.json()["list"][0]["Identifier"]
        logging.debug("%s*****searchtext*******", searchtext)
        highout = {"Identifier": searchtext}
        for mykey, theirkey in highremapper.items():
            highout[mykey] = (
                highresponse.json()["list"][0][theirkey]
                if theirkey in highresponse.json()["list"][0]
                else ""
            )
        out = SingleGroupedItem(highout)

        # Now get all the low items grouped under the group record.
        offset = ""
        done = False
        while not done:
            url = self._getUrl(
                lowtable,
                lowfields,
                formula=searchtext,
                offset=offset,
                maxrecords=maxrecords,
                sort=sort,
            )
            lowresponse = requests.get(url, headers=self.headers)
            if lowresponse.status_code != 200:
                return EnhancedResponse(
                    url,
                    lowresponse,
                    dbasename=self.friendlyname,
                    apikey=self.airTableBaseAPI,
                )
            else:
                if "offset" in lowresponse.json():
                    offset = lowresponse.json()["offset"]
                else:
                    done = True
                if "list" in lowresponse.json():
                    for rec in lowresponse.json()["list"]:
                        remapped = {}
                        for mykey, theirkey in lowremapper.items():
                            if theirkey in rec:
                                remapped[mykey] = rec[theirkey]
                        out.addFields(rec["ID"], remapped)
        # Need to parse the object's data now.

        out.generateTurtle()
        out.generateRDF()
        out.generateOntologyGraph()
        out.generateInstanceGraph()
        return out

    def getListOfGroups(self, schema, maxrecords=None, sort=None):
        """
        Schema is a two-entry dictionary containing tablename:{fields}, that identify the paired Group and Base Data; i.e.
             {
                'Model_fields': {
                    'KeyField': 'Name',
                    'GroupBy': 'Model',
                    'Turtle RDF': 'Model_Fields_Total_Turtle',
                    'CRM Path': 'CRM Path',
                    'Description': 'Description',
                    'Name': 'Field Name',
                    'Identifier': 'Name'
                },
                'Model': {
                    'KeyField': 'ID',
                    'Turtle RDF': 'Model_Turtle_Prefix',
                    'Description': 'Description',
                    'Name': 'Name',
                    'Identifier': 'Identifier'
                }
            }
        The lower-level data can be identified because it contains a GroupBy field, which is mandatory.
        """
        out = []
        lowtable = None
        hightable = None

        for tablename, fieldlist in schema.items():
            # of the two item pairs, need to exclude the "low", so we ignore the one with "GroupBy" in it
            if "GroupBy" in fieldlist:
                lowtable = tablename
            else:
                highremapper = fieldlist
                highfields = list(fieldlist.values())
                hightable = tablename

        """ The key field for the high table is what we use for SEARCH(). """
        pass
        """
            There is always a field in the high list that has the same name as the low table, which contains a list
            of references. So if the list is empty, there are no matching low fields in the category. We want that list
            so we can skip the hyperlink.
        """
        if lowtable is not None and lowtable not in highfields:
            highfields.append(lowtable)
            highremapper["Contains"] = lowtable

        if hightable is None:
            return out

        offset = ""
        done = False
        while not done:
            url = self._getUrl(
                hightable, highfields, offset=offset, maxrecords=maxrecords, sort=sort
            )
            highresponse = requests.get(url, headers=self.headers)
            # logging.debug('%s*****list-response*******', highresponse.json()["list"])
            if highresponse.status_code != 200:
                return EnhancedResponse(
                    url,
                    highresponse,
                    dbasename=self.friendlyname,
                    apikey=self.airTableBaseAPI,
                )
            else:
                if "offset" in highresponse.json():
                    offset = highresponse.json()["offset"]
                else:
                    done = True
                if "list" in highresponse.json():
                    for rec in highresponse.json()["list"]:
                        remapped = {}
                        for mykey, theirkey in highremapper.items():
                            # logging.debug('%s*****mykey*******', mykey)
                            # logging.debug('%s*****theirkey*******', rec[theirkey])
                            remapped[mykey] = rec[theirkey] if theirkey in rec else ""
                        out.append(remapped)
        return out

    def getsinglecall(self, tablename, fieldlist, maxrecords=None):
        """
        A single call (not counting offset loops) to the AirTable, returning a JSON set of results.
        """
        table = tablename
        fields = fieldlist.values()
        # sort = self.fieldNameMap["aggregate"]["Full Name"]
        sort = None
        offset = ""
        done = None
        out = {}
        while not done:
            url = self._getUrl(
                table, fields, sort=sort, offset=offset, maxrecords=maxrecords
            )
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:  # 200: Success
                return EnhancedResponse(
                    url,
                    response,
                    dbasename=self.friendlyname,
                    apikey=self.airTableBaseAPI,
                )
            else:
                if "offset" in response.json():
                    offset = response.json()["offset"]
                else:
                    done = True
                out.update(self._iterateResponse(response, fieldlist))
        return out

    def getsinglerecord(self, tablename, fieldlist, maxrecords=None, offset=None):
        """
        A single call to the AirTable, returning the unprocessed JSON result from AirTable.
        """
        table = tablename
        fields = fieldlist.values()
        # sort = self.fieldNameMap["aggregate"]["Full Name"]
        sort = None
        url = self._getUrl(
            table, fields, sort=sort, offset=offset, maxrecords=maxrecords
        )
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:  # 200: Success
            return EnhancedResponse(
                url, response, dbasename=self.friendlyname, apikey=self.airTableBaseAPI
            )
        else:
            return response

    def _iterateResponse(self, response, fieldlist):
        out = {}
        for rec in response.json()["records"]:
            data = {}
            for flabel, fname in fieldlist.items():
                data[flabel] = rec["fields"][fname] if fname in rec["fields"] else ""
                if isinstance(data[flabel], list):
                    if flabel == "Turtle RDF":
                        data[flabel] = "\n".join(data[flabel])
                    else:
                        data[flabel] = data[flabel][0]
            if "CRM Path" in data:
                data["CRM Path"] = self._fixarrows(data["CRM Path"])
            if "Turtle RDF" in data:
                data["Turtle RDF"] = TurtleCodeBlock(data["Turtle RDF"])

            out[rec["id"]] = data
        return out

    def _getUrl(
        self, table, fieldlist=[], formula=None, sort=None, offset=None, maxrecords=None
    ):
        url = "http://nocodb:8080/api/v1/db/data/v1/Zellij/" + table
        urlparams = []
        if sort:
            urlparams.append(urlencode({"sort[0][field]": sort}))
        if fieldlist:
            for f in fieldlist:
                urlparams.append(urlencode({"fields[]": f}))
        if offset:
            urlparams.append("offset=" + offset)
        if maxrecords:
            urlparams.append("maxRecords=" + str(maxrecords))
        if formula:
            urlparams.append(urlencode({"where": "(ID,like," + str(formula) + ")"}))
        if urlparams:
            url += "?" + "&".join(urlparams)
        return url

    def _fixarrows(self, txt):
        """Convert text arrows to Unicode arrows"""
        return re.sub(r"\-\>", "→", txt)


class EnhancedResponse(object):
    def __init__(self, url, response, dbasename="", apikey="", custom={}):
        self.url = url
        self.response = response
        self.dbasename = dbasename
        self.apikey = apikey
        self.custom = custom
        self.status_code = response.status_code
        self.message = ""
        err = response.json()["error"]
        if isinstance(err, dict):
            self.type = err["type"] if "type" in err else str(err)
            self.message = err["message"] if "message" in err else ""
        else:
            self.type = err

        self.urlparse = urlparse(url)
        chunks = self.urlparse.path.split(self.apikey)
        self.urlpreapi = chunks[0]
        self.urltable = chunks[1][1:]  # leading slash

        self.urlparams = {}
        for p in self.urlparse.query.split("&"):
            k, v = p.split("=")
            if unquote_plus(k) not in self.urlparams:
                self.urlparams[unquote_plus(k)] = []
            self.urlparams[unquote_plus(k)].append(unquote_plus(v))

    def __str__(self):
        txt = "<EnhancedResponse [" + str(self.status_code) + "]>"
        if self.message:
            txt += " " + self.message
        txt += "\n"
        if "http" in self.urlparse.scheme:
            txt += self.urlparse.scheme + "://"
        txt += self.urlparse.path + self.urlparse.params
        if self.urlparse.query:
            txt += "..." + str(self.urlparams.keys())
        return txt


class AirTableError(Exception):
    def __init__(self, url, response):
        self.url = url
        self.response = response
