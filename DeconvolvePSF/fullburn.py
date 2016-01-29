#!/nfs/slac/g/ki/ki06/roodman/Software/anaconda/bin/python
#@Author Sean McLaughlin
desc ='''
This function is a wrapper for the main afterburner module.

It aggregates all exposures available for DES, and executes afterburner on them all.

TODO information on the inputs.
'''

from argparse import ArgumentParser
parser = ArgumentParser(description = desc)

#TODO add a verbose flag
#TODO number of cores, particular queue, etc?
#May want to rename to tmp, since that's what the files really are.
parser.add_argument('outputDir', metavar = 'outputDir', type = str, help =\
                    'Directory to store outputs.')

args = vars(parser.parse_args())

#Ensure provided dir exists
from os import path, mkdir
from glob import iglob
from subprocess import call

if not path.isdir(args['outputDir']):
    raise IOError("The directory %s does not exist."%args['outputDir'])

if args['outputDir'][-1]  != '/':
    args['outputDir']+='/'

data_dir = '/nfs/slac/g/ki/ki18/des/cpd/psfex_catalogs/SVA1_FINALCUT/psfcat/' #Dir where the exposures are

expids = []
for bigdir in iglob(data_dir+ '*/'):
    for littledir in iglob(bigdir+'*/'):
        expids.append(int(littledir[2:-1]))

print len(expids)

from sys import exit
exit(0)

mkdirPermission = True

#I can either call this in threads
#or I can just write bsub files and submit them.

for expid in expids:

    #Make new dir to store files from this run
    if not path.isdir(args['outputDir']+'00%d/'%expid) and mkdirPermission:
        try:
            mkdir(args['outputDir']+'00%d/'%expid)
        except OSError:
            print 'Failed making directory; using original output directory.'
            mkdirPermission = False
        else:
            args['outputDir']+='00%d/'%expid
    else:
        args['outputDir']+='00%d/'%expid

    bsub_filename = args['outputDir']+'%s.bsub'%expid

    with open(bsub_filename) as bsub_file:
        #TODO ask Chris how to get rid of the specific queue and how to specifcy memorty requirements
        bsub_file.write('\n'.join(['#!/bin/bash',
                                '#BSUB -q kipac-ibq',
                                '#BSUB -W 4:00',
                                '#BSUB -J %sPSF'%expid,
                                '#BSUB -oo /nfs/slac/g/ki/ki18/des/swmclau2/DeconvOutput/%s.out'%expid,
                                '#BSUB -n 2']))

        bsub_file.write('python afterburner.py %s %s'%(expid, args['outputDir']))

    bsub_return= call('bsub < %s'%bsub_filename, shell = True)
    bsub_success = True if bsub_return==0 else False

    if not bsub_success:
        #TODO do something
        pass

    #from afterburner import afterburner

    #afterburner(expid, args['outputDir'])