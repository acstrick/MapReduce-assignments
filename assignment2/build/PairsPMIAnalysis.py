import os
import re
import math
import operator


path = os.listdir('pa/')
pairs = {}
pairsC = {}
pairsPMI = {}
pairs2 = []
numLines = 156215.0
denom = 0.0
numer = 0.0

for file in path:      
    f = open('pa/'+file)
    for line in f:
        #pairs2.append(re.split('( |, |)', line)) #adds ['the', 'girl', '3']
        pairs2.append(line.split())
        for i,j,k in pairs2:
            if j == ('*)'):
                pairsC[i+j] = k
            elif float(k) > 9.0:
                pairs[i+j] = k
        #pairs2.append(line.split())
        pairs2 = []
    print "Finished."
    f.close()

#print pairsC

#Calculate PMI for each pair
check = 0
test = []
for key,value in pairs.iteritems():
    numer = float(value)/numLines
    words = []
    words.append(key.replace(')', ',').split(','))
    #print words
    for w1,w2,w3 in words:
        if pairsC.get(w1+',*)', 'bad') != 'bad':
            if pairsC.get('('+w2+',*)', 'bad') != 'bad':
                denom = float(pairsC[w1+',*)'])*float(pairsC['('+w2+',*)'])/numLines
                if w1 == "(cloud":
                    test.append(w1+','+w2+')')
                if w1 == "(love":
                    test.append(w1+','+w2+')')
        else:
            denom = 0.000000000001
            check += 1
    pairsPMI[key] = math.log(numer/denom)
print check
print len(pairs)
print len(pairsPMI)

print len(test)
test.sort()
testDict = {}
for key in test:
    testDict[key] = pairsPMI.get(key)

print sorted(testDict.iteritems(), key=operator.itemgetter(1))

sorted_x = sorted(pairsPMI.iteritems(), key=operator.itemgetter(1))
#print sorted_x[:100]



#print pairs
#print pairs

'''
for pair, count in pairs:
    if count > 9:
        numer = count/numLines
        denom = (pairs[pair(0)+ '*']/numLines) * (pairs[pair(1)+'*']/numLInes)
        pmi = log(numer/denom)
        pairsPMI[pair] = pmi
        
print "The dictionary was size", len(pairs),"and as PMIs", len(pairsPMI)'''
print "Finished."


