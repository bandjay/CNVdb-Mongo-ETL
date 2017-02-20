__author__ = 'm088378'
import sys, os

######### ARG FUNCTIONS ############
def is_valid_file(parser, arg):
    arg = os.path.abspath(arg)
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg


############### My FUNCTIONS #################

### Accept a dictonary, key & index, collapse to a list
def collapseAttributesToList( myDict, k, idx):
    clist = list(set([x[idx] for x in myDict[k]]))
    if 'null' in clist:
        clist.remove('null')
    return clist

def collapseAttributesToSingle( myDict, k, idx):
    clist = list(set([x[idx] for x in myDict[k]]))
    if 'null' in clist:
        clist.remove('null')
    if len(clist) > 1:
        print "ERROR: TOO MANY VALUES"
        print "Index = "+str(idx)
        print myDict[k]
        sys.exit(1)
    if len(clist) < 1:
        return ''
    else:
        return clist[0]

def collapseAttributesToJoin( myDict, k, idx):
    clist = list(set([x[idx] for x in myDict[k]]))
    if 'null' in clist:
        clist.remove('null')
    if len(clist) < 1:
        return ''
    else:
        return ';'.join(clist)

## Ensure var is not empty
def checkToSave(x):
    if type(x) is list:
        if len(x) > 0:
            return True
        else:
            return False
    elif type(x) is str:
        if x == '':
            return False
        else:
            return True


def strToLog2Ratio(s):
    lgr = 0
    if s.lower() == "deletion":
        lgr = -2
    elif s.lower() == "loss":
        lgr = -1
    elif s.lower() == "gain":
        lgr = 1
    elif s.lower() == "duplication":
        lgr = 2
    return lgr



