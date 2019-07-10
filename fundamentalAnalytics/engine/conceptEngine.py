'''
Created on Jul 9, 2019

@author: afunes
'''
from dao.dao import Dao
from modelClass.concept import Concept


class ConceptEngine(object):

    def getOrCreateConcept(self, conceptName, session):
        concept = Dao().getConcept(conceptName, session = session)
        if(concept == None):
            concept = Concept()
            concept.conceptName = conceptName
            Dao().addObject(objectToAdd = concept, session = session, doFlush = True)
        return concept