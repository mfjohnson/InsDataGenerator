from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring
from pyspark import HiveContext
from pyspark import Row
from pyspark import *
from faker import Factory
import os

def dict_to_xml(tag, d):
    '''
    Turn a simple dict of key/value pairs into XML
    '''
    elem = Element(tag)
    for key, val in d.items():
        child = Element(key)
        child.text = str(val)
        elem.append(child)
    return(elem);

def createCreateClaimsSubmission(i):
    claimsSubmit = {
        "PolicyId": "1234-ABC-12ZQ-{0}".format(i),
        "PolicyHolder": "aName",
        "ClaimType":"SomeType",
        "IncidentDate":"adate",
        "IncidentDescription":"ipsofapso",
        "ClaimAmount":1234,
        "SubmittedBy":"John Doe"
    }
    xmlClaim=tostring(dict_to_xml("claim", claimsSubmit))
    return(xmlClaim);

#---------------------------------------------------------
# MAIN PROCESSOR
#---------------------------------------------------------
if __name__ =='__main__':
    print (os.getlogin())
    print (os.getenv("SPARK_HOME","spark home"))

    fileName = "claims.xml"

    #----------------------------
    sc = SparkContext(appName = "Write XML Ins Claims Files")


    a = sc.parallelize(range(1,10))
    claimsList = a.map(lambda i: createCreateClaimsSubmission(i) ).collect()

    f = open(fileName, 'w')
    for claim in claimsList:
        f.write(claim)

    print("----------- File Successfully save to {0}".format(fileName))




