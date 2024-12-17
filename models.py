from sqlalchemy import Column, String, Float, Date, ForeignKey, Integer, Boolean, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Bond(Base):
    __tablename__ = 'bonds'
    # __table_args__ = {'schema': 'serakate'}
    name = Column(String(20), unique=True)
    url = Column(String(100), unique=True)
    isin = Column(String(20), primary_key=True)
    price = Column(Float, nullable=True)
    bid = Column(Float, nullable=True)
    offer = Column(Float, nullable=True)
    nkd = Column(Float)
    nominal = Column(Float)
    rating = Column(String(10), nullable=True)
    end_date = Column(Date, nullable=True)
    oferta = Column(Date, nullable=True)
    oferta_price = Column(Float, nullable=True)
    unknown_coupons = Column(Boolean, nullable=True)
    floating = Column(String(3), nullable=True)
    coupons = relationship('Coupon', back_populates='bond', cascade='all, delete-orphan', passive_deletes=True)

class Coupon(Base):
    __tablename__ = 'coupons'
    # __table_args__ = {'schema': 'serakate'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    # isin = Column(String(20), ForeignKey('serakate.bonds.isin'), index=True)
    isin = Column(String(20), ForeignKey('bonds.isin'), index=True)
    coup = Column(Float, nullable=True)
    temp = Column(Boolean, default=False)
    payday = Column(Date, nullable=True)
    amort = Column(Float, default=0)
    bond = relationship('Bond', back_populates='coupons', passive_deletes=True)
    __table_args__ = (
        UniqueConstraint('isin', 'payday', name='uq_coupon_isin_payday'),
    )

# class Calc(Base):
#     __tablename__ = 'calc'
    