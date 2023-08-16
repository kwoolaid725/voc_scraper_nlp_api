from .database import Base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Table
from sqlalchemy.orm import relationship, backref
from datetime import datetime
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text


retailer_item = Table('item_retailer', Base.metadata,
        Column('retailer_id', ForeignKey('retailers.id'), primary_key=True),
        Column('item_id', ForeignKey('items.id'), primary_key=True),
        Column('url', String, nullable=False)
                      )

class Item(Base):
    __tablename__ = 'items'

    id = Column(Integer, primary_key=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    image = Column(String)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text('now()'))

    retailers = relationship("Retailer", secondary="item_retailer", back_populates="items")

class Retailer(Base):
    __tablename__ = 'retailers'

    id = Column(Integer, primary_key=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    description = Column(String)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text('now()'))

    items = relationship("Item", secondary="item_retailer", back_populates="retailers")

class Review(Base):
    __tablename__ = 'reviews'

    sent_id = Column(Integer, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False, index=True)
    retailer = Column(String, ForeignKey('retailers.name'), nullable=False)
    item = Column(String, ForeignKey('items.name'), nullable=False)
    rating = Column(Integer, nullable=False)
    post_date = Column(DateTime, nullable=False)
    reviewer_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    content = Column(String, nullable=False)
    review = Column(String, nullable=False)
    review_clean = Column(String, nullable=False)
    lemmatized = Column(String)
    ngrams = Column(String)
    neg = Column(Float)
    neu = Column(Float)
    pos = Column(Float)
    keyword = Column(String)
    sentences = Column(String)
    neg_sent = Column(Float)
    neu_sent = Column(Float)
    pos_sent = Column(Float)
    word_count = Column(Integer)
    keywords_yake = Column(String)
    keywords_yake_scor = Column(String)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text('now()'))






