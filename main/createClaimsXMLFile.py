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

def createCreateClaimsSubmission():
    claimsSubmit = {
        "PolicyId": 1234,
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

    tableName = "CUSTOMER_INFO4"

    #----------------------------
    sc = SparkContext(appName = "WriteRDD")
    hc = HiveContext(sc)


    #----------------------------
    rowCountPerGroup = 10
    rowDef = Row("transGroup","CustomerId","CompanyName","Name","EMAIL","Address","CustomerSince","AnnualSales","LastOrderDate")
    fake = Factory.create()


    a = sc.parallelize(range(1,1000))
    rddRows = a.map(lambda i: createCreateClaimsSubmission(i) )
    print("----------- Using the predefined schema")

print("----------- Using the inferred schema")
#df = hc.inferSchema(rddRows)
df = hc.createDataFrame(rddRows)

df.printSchema()
print("----------- \n\n\n\n")



