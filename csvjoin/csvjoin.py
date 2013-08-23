"""
  csvjoin.py
  Copyright (c) 2013 Lorcan Coyle, http://lorcancoyle.org
  License:       MIT License
  Documentation: https://github.com/lorcan/CSVJoin
"""
import argparse
import csv
import redis
import sys
import copy

parser = argparse.ArgumentParser(description='Takes two CSV files and attempts to join them based on the key values. The headers from the first file will be retained unchanged but those from the second will be prefixed with the specified prefix value')
parser.add_argument("firstfile", help="This is the first CSV file.")
parser.add_argument("secondfile", help="This is the second CSV file.")
parser.add_argument("firstkey", help="This is the name of the header from the first file used for the join")
parser.add_argument("secondkey", help="This is the name of the header from the second file used for the join")
parser.add_argument("--firstprefix", default="", help="This is prefix to be used for headers in the first file.")
parser.add_argument("--secondprefix", default="", help="This is prefix to be used for headers in the second file.")
parser.add_argument("outputfile", help="This is the output file, where the joined file will be stored.")

args = parser.parse_args()

def checkAllUnique(array):
  sortedCopy = copy.copy(array)
  sortedCopy.sort()
  lastItem = None
  for item in sortedCopy:
    if(lastItem is not None and lastItem == item):
      return False
    lastItem = item
  return True

r = redis.StrictRedis(host='localhost', port=6379, db=0)
# Defensive Flush
r.flushdb()

outputheader = []

with open(args.firstfile, 'r') as csvfile:
  reader = csv.reader(csvfile)
  first = True
  joinColumnNumber = -1
  for row in reader:
    if first:
      first = False
      firstheader = row
      if args.firstkey not in firstheader:
        print "There is no column called " + args.firstkey + " in the first files's header " + str(firstheader) + ". Unable to join. Exiting."
        sys.exit()
      joinColumnNumber = firstheader.index(args.firstkey)
      for h in firstheader:
        outputheader.append(h)
    else:
      joinKey = row[joinColumnNumber]
      for entry in row:
        r.lpush(joinKey, '' + entry)
    
outputfile = csv.writer(open(args.outputfile, 'w'))

joinCount = 0
with open(args.secondfile, 'r') as csvfile:
  reader = csv.reader(csvfile)
  first = True
  joinColumnNumber = -1
  for row in reader:
    if first:
      first = False
      secondheader = row
      if args.secondkey not in secondheader:
        print "There is no column called " + args.secondkey + " in the second file's header " + str(secondheader) + ". Unable to join. Exiting."
        sys.exit()
      joinColumnNumber = secondheader.index(args.secondkey)
      for h in secondheader:
        outputheader.append(args.secondprefix + h)      
      if(not checkAllUnique(outputheader)):
        print "There are duplicate headers in the output. This won't do. Set a prefix to avoid this. Exiting."
      outputfile.writerow(outputheader)
    else:
      secondFileKey = row[joinColumnNumber]
      if(r.exists(secondFileKey)):
        outputRow = []
        while r.llen(secondFileKey) > 0:
          firstFileEntry = r.rpop(secondFileKey)
          outputRow.append(firstFileEntry)
        for entry in row:
          outputRow.append(entry)
        outputfile.writerow(outputRow)
        joinCount = joinCount + 1
      
print "Joined " + str(joinCount) + " records."
r.flushdb()
