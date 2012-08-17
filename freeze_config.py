#!/usr/bin/env python

import imp
import sys
import pickle

if len(sys.argv) != 3:
  print "Usage: %s <python config file> <output file>" % sys.argv[0]

fileName = sys.argv[1]
outFileName = sys.argv[2]

handle = open(fileName, 'r')
try:
   try:
       print "Importing file %s" % fileName
       cfo = imp.load_source("pycfg", fileName, handle)
       cmsProcess = cfo.process
   except Exception, ex:
       print "Your pycfg file is not valid python: %s" % str(ex)
       raise ex
finally:
   handle.close()

# Write out new config file
pklFileName = outFileName + '.pkl'
outFile = open(outFileName,"w")
print "Writing bootstrap file %s" % outFile
outFile.write("import FWCore.ParameterSet.Config as cms\n")
outFile.write("import pickle\n")
outFile.write("process = pickle.load(open('%s', 'rb'))\n" % pklFileName)
outFile.write("process.RandomNumberGeneratorService.externalLHEProducer.initialSeed = 100\n")
outFile.close()

print "Writing data file %s" % pklFileName
pklFile = open(pklFileName,"wb")
myPickle = pickle.Pickler(pklFile)
myPickle.dump(cmsProcess)
pklFile.close()

print "Done!  You will need to add 'transfer_input_files = %s, %s' to your Condor submit file." % (outFileName, pklFileName)
