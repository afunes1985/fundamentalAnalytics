'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey, Table
from sqlalchemy.sql.sqltypes import Integer

from modelClass import PersistenObject, Base

# 
# association_table = Table('fa_custom_concept_concept_relation', Base.metadata,
#     Column('customConceptOID', Integer, ForeignKey('fa_custom_concept.OID')),
#     Column('conceptOID', Integer, ForeignKey('fa_concept.OID'))
# )

class RelCustomConceptConcept(Base):
    __tablename__ = 'fa_custom_concept_concept_relation'
    customConceptOID = Column(Integer, ForeignKey('fa_custom_concept.OID'), primary_key=True)
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'), primary_key=True)
    order_ = Column(Integer, nullable=True)
    concept = relationship("Concept")

class CustomConcept(PersistenObject):
    __tablename__ = 'fa_custom_concept'
    conceptName = Column(String(250), nullable=False)
    label = Column(String(200), nullable=False)
    customFactList = relationship("CustomFact", back_populates="customConcept")
    expressionList = relationship("Expression", back_populates="customConcept")
    defaultOrder = Column(Integer, nullable=False)
    defaultCustomReportOID = Column(Integer, ForeignKey('fa_custom_report.OID'))
    defaultCustomReport = relationship("CustomReport", back_populates="customConceptList")
    periodType = Column(String(4), nullable=False)
    fillStrategy = Column(String(45), nullable=False)
    relationConceptList = relationship("RelCustomConceptConcept", order_by="RelCustomConceptConcept.order_")