'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class CompanyQResult(PersistenObject):
    __tablename__ = 'fa_company_q_result'
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship('Company', back_populates="companyQResultList")
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="companyQResultList")
    periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    period = relationship("Period", back_populates="companyQResultList")
    value = Column(Float(), nullable=False)