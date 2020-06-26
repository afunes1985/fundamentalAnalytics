'''
Created on Jul 7, 2019

@author: afunes
'''
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import String, Integer

from modelClass import Base


class ErrorMessage(Base):
    __tablename__ = 'fa_error_message'
    __table_args__ = (
        PrimaryKeyConstraint('errorKey', 'fileDataOID'),
    )
    errorKey = Column(String(45), nullable=False)
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    fileData = relationship("FileData", back_populates="errorMessageList")
    errorMessage= Column(String(45), nullable=False)
    extraData= Column(String(45), nullable=False)
