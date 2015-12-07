import sys
f1=sys.argv[1]
f2=sys.argv[2]

def formLineNumberSet(fname):
	lineNums=set()
	with open(fname) as fObject:
		for lines in fObject:
			lineNums.add(lines.split(',')[1])

	return lineNums


n1=formLineNumberSet(f1);
n2=formLineNumberSet(f2);
if (n1 == n2):
	print "Both files have same line numbers "
else:
	print "Both files have different line numbers"
	print n1-n2
	print
	print n2-n1
