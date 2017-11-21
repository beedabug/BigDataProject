"""
    Program find the word sense disambiguation 
    Author:Chandni Shankar
"""
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize

def initialize(word):
    """
        Function to initialize the best sense and retrieve the definition and examples of each sense    
    """
    bankSyns = wordnet.synsets(word)
    bestSense = ""
    synRelations = dict()
    maxCount = 0
    for syn in bankSyns:
        syndetails = dict()
        count = syn.lemmas()[0].count()
        syndetails["count"] = count
        syndetails["definition"] = word_tokenize(syn.definition())
        examples = syn.examples()
        egWords = []
        for example in examples:
            egWords.extend(word_tokenize(example))
        syndetails["examples"] = egWords
        synRelations[syn.name()] = syndetails
        if count > maxCount:
           bestSense = syn
           maxCount = count
    return synRelations, bestSense

def computeOverlap(signature, context):
    """
        Function to overlap for sense     
    """
    overlap = 0
    for word in context:
        for ref in signature:
            if str(word) == str(ref):
                overlap += 1
                break
    return overlap

def desk(context, synRelations, intBestSense):
    """
        Function to apply the Lesk algorithm and find the best sense    
    """
    bestSense = intBestSense
    maxOverlap = 0
    overLapDict = dict()
    for sense, relations in synRelations.iteritems():
        signature = relations["definition"]
        signature.extend(relations["examples"])
        overlap = computeOverlap(signature, context)
        overLapDict[sense] = overlap
        if overlap > maxOverlap:
            maxOverlap = overlap
            bestSense = sense
    return bestSense, overLapDict

sentence = "The bank can guarantee deposits will eventually cover future tuition costs because it invests in adjustable-rate mortgage securities."
context = word_tokenize(sentence)
synRelations, intBestSense = initialize("bank")
bestSense, overlap = desk(context, synRelations, intBestSense)
print "Best Sense: ", bestSense
print '{:^40}'.format("Sense"), '{:^40}'.format("Overlap")
for key in overlap:
    print '{:^40}'.format(key), '{:^40}'.format(overlap[key])
        
        
