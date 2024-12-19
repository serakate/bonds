from sqlalchemy import Column, String, Float, Date, ForeignKey, Integer, Boolean, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Bond(Base):
    __tablename__ = 'bonds'
    # __table_args__ = {'schema': 'serakate'}

    # Основные поля
    isin = Column(String(20), primary_key=True)
    name = Column(String(20), unique=True, nullable=True)  # краткое имя (SHORTNAME/short)
    full_name = Column(String(100), nullable=True)  # полное имя с сайта рейтингов

    # Торговые параметры
    price = Column(Float, nullable=True)  # MARKETPRICETODAY/last_price
    bid = Column(Float, nullable=True)  # только с MOEX
    offer = Column(Float, nullable=True)  # только с MOEX
    nkd = Column(Float, nullable=True)  # ACCRUEDINT/nkd
    nominal = Column(Float, nullable=True)  # LOTVALUE/dolg
    
    # Параметры облигации
    rating = Column(String(10), nullable=True)  # только с сайта рейтингов
    status = Column(String(20), nullable=True)  # только с сайта рейтингов
    end_date = Column(Date, nullable=True)  # MATDATE/maturity_date
    oferta = Column(Date, nullable=True)  # OFFERDATE/offer_date
    oferta_price = Column(Float, nullable=True)  # BUYBACKPRICE/buyback_price
    
    # Служебные поля
    unknown_coupons = Column(Boolean, nullable=True)
    floating = Column(String(3), nullable=True)
    
    # Связи
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

class KvalBond(Base):
    __tablename__ = 'kval_bonds'
    isin = Column(String(20), primary_key=True)
    name = Column(String(20), unique=True, nullable=True)
    full_name = Column(String(100), nullable=True)

# class Calc(Base):
#     __tablename__ = 'calc'
    