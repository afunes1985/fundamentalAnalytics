import logging

import pandas

from dao.dao import DaoFAConcept
from modelClass.faconcept import FAConcept

import xml.etree.ElementTree
e = xml.etree.ElementTree.parse('us-gaap-lab-2018-01-31.xml').getroot()
 
for element1 in e.findall("{http://www.xbrl.org/2003/linkbase}labelLink"):
    for element2 in element1.findall("{http://www.xbrl.org/2003/linkbase}label"):
        elementID = element2.attrib["id"]
        label = element2.text
        #print(elementID, label)
        indicatorID = elementID[4: elementID.find("_label_en-US" , 4, len(elementID))]
        faConcept = FAConcept(None)
        faConcept.setAttr(None, None, indicatorID, label)
        try:
            rs = DaoFAConcept.getConceptByIndicatorID(indicatorID)
            if len(rs) == 0:
                DaoFAConcept.insertFAConcept(faConcept)
                #print(faConcept.__dict__)
            else:
                DaoFAConcept.updateLabelByIndicatorID(indicatorID, label)
                #print(indicatorID, label)
        except Exception as e:
            logging.warning(e)

    
# from xml.dom import minidom
# xmldoc = minidom.parse('us-gaap-lab-2018-01-31.xml')
# itemlist = xmldoc.getElementsByTagName('link:linkbase')
# print(len(itemlist))
# print(xmldoc.childNodes[0])
# for s in xmldoc.childNodes:
#     print(s)    

if (1 == 0):
    section = 'INCOME'
    xml = file('income.xsd').read()
    for row in xml.split('>'):
            #print(row)
            firstPoint = row.find("loc_us-gaap_", 0,len(row)) + len("loc_us-gaap_")
            lastPoint= row.find("_", firstPoint,len(row))
            itemName = row[firstPoint : lastPoint]
            faConcept = FAConcept(None)
            faConcept.setAttr(None, section, itemName)
            if (1 == 1):
                try:
                    DaoFAConcept.insertFAConcept(faConcept)
                    print(row[firstPoint : lastPoint])
                except Exception as e:
                    logging.warning(e)

if (1 == 0):
    df = pandas.read_excel('C://Users//afunes//iCloudDrive//PortfolioViewer//fundamentalConcepts.xlsx');
    #get the values for a given column
    sectionValues = df['section'].values
    conceptValues = df['concept'].values
    labelValues = df['description'].values
    for index, conceptValue in enumerate(conceptValues):
        indicatorID = conceptValues[index]
        section = sectionValues[index]
        label = labelValues[index]
        faConcept = FAConcept(None)
        faConcept.setAttr(None, section, indicatorID, label)
        if (1 == 1):
            try:
                rs = DaoFAConcept.getConceptByIndicatorID(indicatorID)
                if len(rs) == 0:
                    DaoFAConcept.insertFAConcept(faConcept)
                    print(faConcept.__dict__)
                else:
                    #print(rs[0][0])
                    DaoFAConcept.updateLabelByOID(rs[0][0], label)
                    print(faConcept.__dict__)
            except Exception as e:
                logging.warning(e)