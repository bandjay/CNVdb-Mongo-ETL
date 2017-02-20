__author__ = 'm088378'
import sys

############### FUNCTIONS #################

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
