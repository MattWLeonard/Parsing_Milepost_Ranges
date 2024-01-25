## GOAL: parse out milepost ranges from locations given in Accuweather warning data

################################################################################

import arcpy as a
from arcpy import env
##import sys
##import datetime


# access current mxd
a.mapping.MapDocument('CURRENT')   # use if running within ArcMap
mxd = a.mapping.MapDocument('CURRENT')   # use if running within ArcMap


# set input/output location (to be workspace)
gdb = ""										# <<<<<<<<<<<<<<<<<< FILL IN FILE PATH TO WORKSPACE GDB

# set workspace
a.env.workspace = gdb


# last date of most recent data received from Accuweather
enddate = ""	# <<<<<<<<< FILL IN DATE FOR MOST RECENT DATA RECEIVED (like "20220930")


# input excel file (to be used in ExcelToTable)
infile = ""				# <<<<<<<<<<<<<< FILL IN - full file path and file name with extension (.xls)


# template for new parsed table (to be used in CreateTable)
template = "C:/MWLlocal/Weather_local/Weather_local.gdb/TEMPLATE_Skyguard_MPranges"	


################################################################################
################################################################################

# excel to table	# DONE
outtable = weathertable = "SkyGuard_Warning_Metrics_" + enddate
sheet = ""	# optional
a.ExcelToTable_conversion(infile,outtable,sheet)



# create new empty table to hold individual location records	# DONE
# ^ uses template table that already exists
outpath = gdb
outname = mptable = "SkyGuard_Warning_MPranges_" + enddate
a.CreateTable_management(outpath,outname,template)



# establish dictionary of divcodes
div_dictionary = {}
div_dictionary["Piedmont"]		="03"
div_dictionary["Georgia"]		="04"
div_dictionary["Alabama"]		="08"
div_dictionary["Pocahontas"]	="62"
div_dictionary["Harrisburg"]	="72"
div_dictionary["Pittsburgh"]	="74"
div_dictionary["Dearborn"]		="76"
div_dictionary["Lake"]			="92"
div_dictionary["Illinois"]		="94"
div_dictionary["Coastal"]		="04"
div_dictionary["Gulf"]			="08"
div_dictionary["Blue Ridge"]	="62"
div_dictionary["Keystone"]		="72"
div_dictionary["Great Lakes"]	="76"
div_dictionary["Midwest"]		="94"


#############

cursordata = 	weathertable
cursorfields = 	[
	"Number",
	"Client",
	"Condition",
	"Issued_Time",
	"Valid_Time",
	"Expire_Time",
	"Warning_Type",
	"Number_of_Locations",
	"Locations",
	]
##whereclause = 	""  # (optional)
with a.da.SearchCursor(cursordata, cursorfields) as cursor:
	for row in cursor:

		locs = row[8]
		
		# convert Locations string to a list of strings
		locslist = locs.split("; ")
		# ^ result will be like ['Coastal SC128.90 TO Coastal SC31.80', 'Coastal SB6.40 TO Coastal SB0.00;']
		
		# remove semicolon from final item in list (tested, works)
		locslist[(len(locslist)-1)] = locslist[(len(locslist)-1)].replace(";","")		
		
		
		# if len of last item in list is 0 (empty string), then remove that item from list
		if "" in locslist:
			locslist.remove("")
		else:
			pass
		
		
		for l in locslist:	# each item will be like 'Coastal SC128.90 TO Coastal SC31.80'
								# (could have suffix, or no pre/suf)
			loc = l
			
			# remove spaces in division names (for splitting into list)
			loc = loc.replace("Great Lakes","GreatLakes")
			loc = loc.replace("Blue Ridge","BlueRidge")
			
			mprange = loc.split()	# result will be like ['Coastal', 'SC128.90', 'TO', 'Coastal', 'SC31.80']
		
			div = mprange[0]
			
			# replace space in division names
			div = div.replace("GreatLakes","Great Lakes")
			div = div.replace("BlueRidge","Blue Ridge")
			loc = loc.replace("GreatLakes","Great Lakes")
			loc = loc.replace("BlueRidge","Blue Ridge")
			
			divcode = div_dictionary[div]
				
			hmp = mprange[4]
			lmp = mprange[1]
			
			hl = len(hmp)
			ll = len(lmp)
			
			# test if there is a prefix
			if hmp[0].isalpha() >0:
				if hmp[1].isalpha() >0:  # prefix is 2 chars
					pre = hmp[0:2]
					hmp = float(hmp[2:hl])
					lmp = float(lmp[2:ll])
					mln = pre + divcode + "__"
				else:  # prefix is 1 char
					pre = hmp[0:1]
					hmp = float(hmp[1:hl])
					lmp = float(lmp[1:ll])
					mln = "_" + pre + divcode + "__"
				suf = ""
			# test if there is a suffix
			elif hmp[hl-1].isalpha() >0:
				if hmp[hl-2].isalpha() >0:  # suffix is 2 chars
					suf = hmp[hl-2:hl]
					hmp = float(hmp[0:hl-2])
					lmp = float(lmp[0:ll-2])
					mln = "__" + divcode + suf
				else:  # suffix is 1 char
					suf = hmp[hl-1:hl]
					hmp = float(hmp[0:hl-1])
					lmp = float(lmp[0:ll-1])
					mln = "__" + divcode + suf + "_"
				pre = ""
			else: # there is no pre/suf
				hmp = float(hmp)
				lmp = float(lmp)
				mln = "__" + divcode + "__"
				pre = ""
				suf = ""
				
			# define mileage length of segment
			length_m = hmp-lmp
			
			
			# write record to new table
			icursordata = 	mptable
			icursorfields = [
				'Number',		# f0
				'Client',		# f1
				'Condition',	# f2, etc.
				'Issued_Time',
				'Valid_Time',
				'Expire_Time',
				'Warning_Type',
				'Number_of_Locations',
				'LOCATION',
				'DIVISION',
				'DIVCODE',
				'PREFIX',
				'LMP',
				'HMP',
				'SUFFIX',
				'MASTERLINENAME',
				'LENGTH_M',
				]
			with a.da.InsertCursor(icursordata, icursorfields,) as icursor:				
				f0 = row[0]
				f1 = row[1]
				f2 = row[2]
				f3 = row[3]
				f4 = row[4]
				f5 = row[5]
				f6 = row[6]
				f7 = row[7]
				f8 = loc
				f9 = div
				f10 = divcode
				f11 = pre
				f12 = lmp
				f13 = hmp
				f14 = suf
				f15 = mln
				f16 = length_m
				
				icursor.insertRow((f0,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16))
			del icursor

del cursor