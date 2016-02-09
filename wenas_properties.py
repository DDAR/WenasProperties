""" #####################################################
	NAME: wenas_properties.py
	Source Name:
	Version: ArcGIS 10.1 - python 2.7
	Author: DD Arnett
	Usage: Check Wenas Water District parcels against current Taxlots
	Required Arguments: none
	Optional Arguments: none
	Description: Used on a regular basis to check for differences.
	Date Created: Oct 10, 2013
##################################################### """
import os
import string
import sys
import arcpy
import time
import pyodbc
from arcpy import env
from datetime import date
from datetime import datetime

mail_recipient = 'donna.arnett@co.yakima.wa.us'
# Methods ----------------------------------------------------------------------

def message(msg):
	LocalTime = time.asctime(time.localtime(time.time()))
	mmsg = msg + LocalTime; arcpy.AddMessage(mmsg); print mmsg

def killObject( object ):
	if arcpy.Exists(object):
		arcpy.Delete_management(object)

# Variables --------------------------------------------------------------------
env.overwriteoutput = True
baseSpatialRecord = r"D:\Data\DDA\Wenas\WenasLands.shp"
taxSpatialRecord = r"M:\Geodatabase\Taxlots\Taxlots.gdb\parcels"
partyTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Party"
propTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Property"
taxquery = """ "PARC" < 60000 """
wTable = {"ASSESSOR_N": '', "NAME": '', "LASTNAME": '', "MAILING": '', "CITY": '', "STATE": '', "ZIPCODE": '', "SITUS": '', "LOOKUP": '', "MAP": ''}


# Methods ----------------------------------------------------------------------
# Find the situs address in the parcel property table
def findSitus(inputID):
    propQry = """ "ASSESSOR_N" = '{0}'""".format(inputID)
    arcpy.MakeTableView_management(propTable, "propparc", propQry)
    rotton = arcpy.SearchCursor("propparc")
    cnt = int(arcpy.GetCount_management("propparc").getOutput(0))
    for rot in rotton:
        if cnt == 1:
            fsSitus = rot.getValue("SITUS_ADDR")

    # Clean up
    killObject("propparc")
    return fsSitus

# Create the hyperlink text and the map lookup text
def createHyper(inputID):
    look = 'http://yes.co.yakima.wa.us/Assessor/parcel_details.aspx?pn=' + inputID
    maplook = 'http://www.yakimap.com/servlet/com.esri.esrimap.Esrimap?name=YakGISH&Cmd=Search&TAB=TabAssessor&SEARCH_BY=Parcel&SearchTextParcel=' + inputID
    return (look, maplook)

# Find the owners and address information from the parcel party table
def findParty(inputID, fpSitus, fpMap, fpLook):
    partyQry = """ "ASSESSOR_N" = '{0}'""".format(inputID)
    arcpy.MakeTableView_management(partyTable, "partyparc", partyQry)
    cnt = int(arcpy.GetCount_management("partyparc").getOutput(0))
    #print 'The number of selected party records: ' + str(cnt)
    if cnt > 0:
        rotton = arcpy.SearchCursor("partyparc")
        for rot in rotton:
            lastname = rot.getValue("LAST_NAME")
            firstName = rot.getValue("FIRST_NAME")
            lastName = rot.getValue("LAST_NAME")
            orgName = rot.getValue("ORG_NAME")
            xName = firstName + " " + lastName + " " + orgName
            mailing = rot.getValue("MAILING_AD")
            city = rot.getValue("MAILING_CI")
            state = rot.getValue("STATE")
            zipC = rot.getValue("ZIP_CODE")
            addTableRec(inputID, xName, lastname, mailing, city, state, zipC, fpSitus, fpLook, fpMap )
    elif cnt == 0:
        print 'no records found'
    else:
        print 'error in finding the party'

    # Clean up
    killObject("partyparc")

# Add a record to the table
def addTableRec(pId, name, lname, mail, city, state, zip, situs, lookup, map):
    #cxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\\Data\\DDA\\Wenas\\WenasRecords.accdb; Provider=MSDASQL;')
    cursor = cxn.cursor()
    insertStr = "INSERT INTO WenasProperties(ASSESSOR_N, NAME, LASTNAME, MAILING, CITY, STATE, ZIPCODE, SITUS, LOOKUP, MAP) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}')".format(pId, name, lname, mail, city, state, zip, situs, lookup, map)
    cursor.execute(insertStr)
    cxn.commit()
    #cxn.close()

def createAccessDB():
    #cxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\\Data\\DDA\\Wenas\\WenasRecords.accdb; Provider=MSDASQL;')
    cursor = cxn.cursor()
    cursor2 = cxn.cursor()
    for table in cursor.tables():
        if table.table_name == "WenasProperties":
            drop = "DROP TABLE [{0}]".format("WenasProperties")
            cursor2.execute(drop)
    string = "CREATE TABLE WenasProperties(ASSESSOR_N varchar(11), NAME varchar(50), LASTNAME varchar(50), MAILING varchar(100), CITY varchar(25), STATE varchar(25), ZIPCODE varchar(10), SITUS varchar(100), LOOKUP varchar(255), MAP varchar(255))"
    cursor.execute(string)
    cxn.commit()
    #cxn.close()

# Program ----------------------------------------------------------------------
try:
    # Delete the current WenasProperties table and create an empty new one
    cxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\\Data\\DDA\\Wenas\\WenasRecords.accdb; Provider=MSDASQL;')
    createAccessDB()
    print 'Database Created'
    # Create a layer (in memory) of parcels
    #sel3Count = int(arcpy.GetCount_management(taxSpatialRecord).getOutput(0))
    arcpy.MakeFeatureLayer_management(taxSpatialRecord, "fullparcels")
    #sel2Count = int(arcpy.GetCount_management("fullparcels").getOutput(0))
    # Select from parcel layer those records that have their centers in base wenas shapefile
    arcpy.SelectLayerByLocation_management("fullparcels", "HAVE_THEIR_CENTER_IN", baseSpatialRecord)
    #selCount = int(arcpy.GetCount_management("fullparcels").getOutput(0))
    # Make a layer of selected records and only include those records that have parc > 60000
    arcpy.MakeFeatureLayer_management("fullparcels", "wenasparcels", taxquery)
    wMemory = "in_memory" + "\\" + "out"
    arcpy.CopyRows_management("wenasparcels", wMemory)
    wParcels = wMemory

    arcpy.DeleteIdentical_management(wParcels, ["ASSESSOR_N"])

    sel4Count = int(arcpy.GetCount_management(wParcels).getOutput(0))
    print 'The number of selected wenas records: ' + str(sel4Count)

    # Cursor through the selected wenas parcel records
    rows = arcpy.SearchCursor(wParcels)
    for row in rows:
        situs = ''
        maplook = ''
        look = ''
        parcID = row.getValue("ASSESSOR_N")
        situs = findSitus(parcID)
        look, maplook = createHyper(parcID)
        findParty(parcID, situs, maplook, look)




    # Clean up
    cxn.close()
    killObject("fullparcels")
    killObject(wParcels)



except arcpy.ExecuteError:
	msgs = arcpy.GetMessages(2)
	print arcpy.AddMessage("There was a problem...script bailing")
	arcpy.AddError(msgs)
	print msgs

#finally:
#	if selobj:
#		del selobj
#	if sel:
#		del sel

