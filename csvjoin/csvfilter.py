"""
  csvfilter.py
  Copyright (c) 2013 Lorcan Coyle, http://lorcancoyle.org
  License:       MIT License
  Documentation: https://github.com/lorcan/CSVJoin
"""
import argparse
import csv
import sys

parser = argparse.ArgumentParser(description='Takes a CSV files a column header and value and generates a new file that does not contain any rows with that column value')
parser.add_argument("inputfile", help="This is the CSV file to be processed.")
parser.add_argument("columnName", help="This is the name of the header to be filtered")
parser.add_argument("columnValue", help="This is the value used for filtering")
parser.add_argument("outputfile", help="This is the name of the file where the output is to be put.")

args = parser.parse_args()

outputfile = csv.writer(open(args.outputfile, 'w'))

filterCount = 0
with open(args.inputfile, 'r') as csvfile:
  reader = csv.reader(csvfile)
  first = True
  filterColumnNumber = -1
  for row in reader:
    if first:
      first = False
      if args.columnName not in row:
        print "There is no column called " + args.columnName + " in the input files's header " + str(row) + ". Unable to filter. Exiting."
        sys.exit()
      filterColumnNumber = row.index(args.columnName)
      outputfile.writerow(row)
    else:
      if(args.columnValue == row[filterColumnNumber]):
        filterCount = filterCount + 1 
        # Do nothing
      else:
        outputfile.writerow(row)

print "Filtered " + str(filterCount) + " records."

