import yaml
import os
import csv
import urllib.request as urllib2
# import urllib2 # i used for online csv
import getopt
import sys
# https://stackoverflow.com/questions/16283799/how-to-read-a-csv-file-from-a-url-with-python

root = os.getcwd()

# takes a csvFile name and output file name/path
def csvToYaml(csvFile, output):
    stream = csvFile(output, 'w')
    csvOpen = csv.reader(csvFile)
    keys = next(csvOpen)
    for row in csvOpen:
        yaml.dump([dict(zip(keys, row))], stream, default_flow_style=False)

# converts single url file
def urlCSV(url, output=None):
    csvFile = urllib2.urlopen(url)
    output = output if output else root+'/'+(url.split('/')[-1].replace('.csv','.yml'))
    csvToYaml(csvFile, output)

# converts all csv file in this folder
def localCSV(folder=root):
    # print folder
    for f in os.listdir(folder):
        if f.endswith('.csv'):
            csvFile = os.path.join(folder, f)
            output = os.path.join(folder, f.replace('.csv','.yml'))
            print (output)
            singleCSV(csvFile, output)

# converts only one csv file
def singleCSV(csvFile, output=None):
    output = output if output else root+'/'+(csvFile.split('/')[-1].replace('.csv','.yml'))
    with open(csvFile, 'rb') as csvFile:
        csvToYaml(csvFile, output)

# print -h --help
def usage():
    print ('\nUsage:\n-i --input: path/link of file (if link use url flag)\n-o --output: path name of output, if left out will convert in this folder using its name as output\n-u --url: url flag indicating input is a url and should be treated as such\n-f --folder: flag indicating input is directory/folder and should be treate as such\n-h --help: print this/help stuff.....')

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hi:o:uf', ['help', 'input=','output=', 'url', 'folder'])
    except getopt.GetoptError as err:
        print (str(err))
        usage()
        sys.exit(2)
    csvFile = None
    output = None
    url = False
    folder = False
    if len(opts) == 0:
        localCSV()
        exit()
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            sys.exit()
        elif o in ('-i', '--input'):
            csvFile = a
        elif o in ('-o', '--output'):
            output = a
        elif o in ('-u', '--url'):
            url = True
        elif o in ('-f', '--folder'):
            folder = True
        else:
            print ('unhandled option')
    if url:
        urlCSV(csvFile, output)
        exit()
    elif folder:
        localCSV(csvFile)
        exit()
    singleCSV(csvFile, output)

if __name__ in ("__main__", "csvyml"):
    main()