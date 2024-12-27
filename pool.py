import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter("ignore", category=ConnectionResetError)

import asyncio
import aiohttp
from collections import Counter
import json
from datetime import datetime, date
import requests
import csv
from pathlib import Path
from sqlalchemy import create_engine, case, func, inspect
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import or_
from tqdm import tqdm
import aiomoex

import time
from models import Bond, Coupon, Base, ExitBond, Calc
from sqlalchemy.exc import IntegrityError


engine = create_engine('sqlite:///pool.db')
if not Path('pool.db').exists():
    Base.metadata.create_all(engine)
else:
        # Проверяем наличие каждой таблицы
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'bonds' not in existing_tables:
        Bond.__table__.create(engine)
    if 'coupons' not in existing_tables:
        Coupon.__table__.create(engine)
    if 'kval_bonds' not in existing_tables:
        ExitBond.__table__.create(engine)
    if 'calc' not in existing_tables:
        Calc.__table__.create(engine)
Session = sessionmaker(bind=engine)
db_session = Session()


token_lock = asyncio.Lock()

inf4day = 0.15  # коэффициент обесценивания денег за счёт инфляции
commission = 0.0006

def update_bond_calculations():
    """
    Обновляет расчетные значения для всех облигаций одним SQL-запросом
    """
    current_date = date.today()

    def get_inf_coef(date_column):
        """Возвращает коэффициент инфляции для указанной даты"""
        return func.power(
            1 - inf4day/365.0,
            func.julianday(date_column) - func.julianday(current_date)
        )

    # Создаем CTE для расчета купонных выплат
    coupon_payments = db_session.query(
        Coupon.isin,
        func.sum(
            case(
                # Купон с НДФЛ + амортизация, умноженные на коэффициент инфляции
                (Coupon.payday > current_date,
                  (Coupon.coup + Coupon.amort) * get_inf_coef(Coupon.payday)),
                else_=0
            )
        ).label('total_income_i'),
        func.sum(
            case(
                # Купон с НДФЛ + амортизация без учета инфляции
                (Coupon.payday > current_date,
                  Coupon.coup + Coupon.amort),
                else_=0
            )
        ).label('total_income')
    ).group_by(Coupon.isin).cte('coupon_payments')

    # Основной запрос
    calc_query = db_session.query(
        Bond.isin,
        # Годы до погашения/оферты
        ((func.julianday(func.coalesce(Bond.oferta, Bond.end_date)) - 
          func.julianday(current_date)) / 365).label('years'),
        
        # Цена покупки с учетом комиссии и НКД
        ((Bond.offer / 100 * Bond.nominal + 
          func.coalesce(Bond.nkd, 0)) * 
         (1 + commission)).label('purchase_price'),
        
        case(
            (Bond.has_amort == False,
             coupon_payments.c.total_income + Bond.nominal * get_inf_coef(
                 func.coalesce(Bond.oferta, Bond.end_date))),
            else_=coupon_payments.c.total_income_i
        ).label('total_income_i'),
        
        case(
            (Bond.has_amort == False,
             coupon_payments.c.total_income + Bond.nominal),
            else_=coupon_payments.c.total_income
        ).label('total_income')
    ).join(
        coupon_payments,
        Bond.isin == coupon_payments.c.isin
    ).filter(
        Bond.offer.isnot(None),
        Bond.nominal.isnot(None),
        func.coalesce(Bond.oferta, Bond.end_date) > current_date
    ).cte('calc_query')
    
    # Финальный запрос с расчетом доходностей
    final_query = db_session.query(
        calc_query.c.isin,
        calc_query.c.purchase_price,
        # Общая доходность
        ((calc_query.c.total_income / 
          calc_query.c.purchase_price - 1) * 100
        ).label('yield_p'),
        
        # Общая доходность с учетом инфляции
        ((calc_query.c.total_income_i / 
          calc_query.c.purchase_price - 1) * 100
        ).label('yield_i'),
        calc_query.c.years
    ).join(
        Bond,
        Bond.isin == calc_query.c.isin
    ).cte('final_query')

    # Создаем подзапрос для вставки/обновления
    insert_query = db_session.query(
        final_query.c.isin,
        final_query.c.purchase_price,
        final_query.c.yield_p,
        final_query.c.yield_i,
        final_query.c.years,
        # Годовая доходность
        (final_query.c.yield_p / final_query.c.years).label('yield_y'),
        # Годовая доходность с учетом инфляции
        (final_query.c.yield_i / final_query.c.years).label('yield_iy')
    ).select_from(
        final_query
    )

    # Обновляем таблицу Calc
    db_session.execute(
        Calc.__table__.delete()
    )
    
    db_session.execute(
        Calc.__table__.insert().from_select(
            ['isin', 'purchase_price', 'yield_p', 'yield_i', 'years', 'yield_y', 'yield_iy'],
            insert_query
        )
    )
    
    db_session.commit()

async def get_rating():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        global token
        headers = {"Authorization": f"Bearer {token}"}
        url = 'https://oko.grampus-studio.ru/api/bonds/?page=1&page_size=20000&order=desc&order_by=id'
        payload = {
            'loose': True,
            'status': ["В обращении", "Аннулирован", "Дефолт"]} 
        
        filename = f'danger_bonds.csv'
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['isin', 'name', 'status'])  # Записываем заголовки

            for _ in range(3):
                try:
                    async with session.post(url, json=payload, headers=headers) as response:
                        code = response.status
                        if code != 200:
                            await asyncio.sleep(1)
                            continue
                        data = await response.json()
                        if data.get('detail') == 'Signature has expired':
                            token = await get_token()
                            data = await response.json()
                        elif not 'results' in data.keys():
                            raise Exception(f"Ошибка: {data}")
                        elif not data['results']:
                            return

                        isin_ratings = {}
                        bond_count = 0

                        for row in data['results']:
                            if row['status'] in ('Аннулирован', 'Дефолт'):
                                writer.writerow([
                                    row['isin'], 
                                    row.get('name', '') or row.get('short', ''), 
                                    row['status']
                                ])
                                bond_count += 1
                                db_session.merge(ExitBond(isin=row['isin'], name=row.get('name', '') or row.get('short', ''), reason=row['status']))
                                db_session.commit()
                                continue
                            
                            ratings = Counter([k['value'] for k in row['ratings'] 
                                               if k['value'] and 'отозван' not in k['value'].lower()])
                            if ratings:
                                row['ratings'] = ratings.most_common(1)[0][0]
                                row.pop('borrower')
                                isin_ratings[row['isin']] = row

                        return isin_ratings
                except (aiohttp.client_exceptions.ClientOSError, 
                        aiohttp.client_exceptions.ServerDisconnectedError):
                    return None 
            return None

async def fetch(session, url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            async with session.get(url) as response:
                status = response.status
                if status == 200:
                    return await response.text()
                else:
                    return None
        except Exception as e:
            pass

        retries += 1
        await asyncio.sleep(1)  # Ожидаем перед повторной попыткой

    return None

def fetch_sync(session, url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url)
            status = response.status_code
            if status == 200:
                return response.text
            else:
                return None
        except Exception as e:
            pass

        retries += 1
        time.sleep(1)  # Синхронное ожидание перед повторной попыткой

    return None


def merge_bond_data(moex_data={}, rating_data={}):
    """
    Объединяет данные из двух источников, предпочитая непустые значения
    """
    result = {}
    
    # Базовые поля
    result['isin'] = moex_data.get('ISIN') or rating_data.get('isin')
    result['name'] = moex_data.get('SHORTNAME', None) or rating_data.get('short', None)
    
    # Торговые параметры
    result['price'] = moex_data.get('MARKETPRICETODAY') or rating_data.get('last_price')
    result['bid'] = moex_data.get('BID')  # только с MOEX
    result['offer'] = moex_data.get('OFFER')  # только с MOEX
    result['nkd'] = moex_data.get('ACCRUEDINT') or rating_data.get('nkd')
    result['nominal'] = moex_data.get('LOTVALUE') or rating_data.get('dolg')
    
    # Параметры облигации
    result['rating'] = ('AAA' if moex_data and 'ОФЗ' in moex_data.get('SHORTNAME', '') 
                       else rating_data.get('ratings') if rating_data 
                       else None)
    result['status'] = rating_data.get('status') if rating_data else None
    
    # Даты
    format = "%Y-%m-%d"
    try:
        result['end_date'] = (datetime.strptime(moex_data.get('MATDATE') or rating_data.get('maturity_date'), format) 
                             if (moex_data.get('MATDATE') or rating_data.get('maturity_date')) else None)
    except (ValueError, TypeError):
        result['end_date'] = None
        
    try:
        oferta_date = moex_data.get('OFFERDATE') or rating_data.get('offer_date')
        result['oferta'] = datetime.strptime(oferta_date, format) if oferta_date and oferta_date != '-' else None
    except (ValueError, TypeError):
        result['oferta'] = None
    
    result['oferta_price'] = moex_data.get('BUYBACKPRICE') or rating_data.get('buyback_price')
    
    return result

async def ask_moex(session, url, kwargs):
    iss = aiomoex.ISSClient(session, url, kwargs)
    data = await iss.get()
    data = [s|m for s, m in zip(data['securities'], data['marketdata'])]
    data = {x['ISIN']: x for x in data if any([x["BID"], x["OFFER"], x["MARKETPRICETODAY"]]) and x["FACEUNIT"] not in ('EUR', 'USD')}
    return data

async def get_token():
    async with token_lock:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            payload = {"email": "serakate@yandex.ru", "password": "bncjdjy"}
            async with session.post('https://oko.grampus-studio.ru/api/login', json=payload, ) as response:
                data = await response.json()
                if 'access_token' in data.keys():
                    return data['access_token']
                else:
                    raise Exception(f"Ошибка: {data}")

async def main():
    global token
    token = await get_token()

    xml_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
    kwargs = {"iss.meta":"off",
              "iss.only": "marketdata,securities",
              "marketdata.columns": "BID,OFFER,MARKETPRICETODAY",   #спрос, предложение, рыночная цена %
              "securities.columns": "ISIN,SHORTNAME,ACCRUEDINT,OFFERDATE,BUYBACKPRICE,LAST,LOTVALUE,MATDATE,FACEUNIT",
              "marketprice_board": "1"  # краткое назв, нкд, дата оферты, цена оферты, цена посл сделки %, номинал, дата погашения, валюта
             }

    async with aiohttp.ClientSession() as session:
        # Получаем данные асинхронно
        print("Получаем данные из MOEX и рейтингов")
        moex_task = ask_moex(session, xml_url, kwargs)
        ratings_task = get_rating()
        
        # Параллельно получаем список квалифицированных и недоступных облигаций
        kval_isins = {isin[0] for isin in db_session.query(ExitBond.isin).all()}
        
        # Дожидаемся получения данных
        moex_data, ratings = await asyncio.gather(moex_task, ratings_task)
        
        # Фильтруем результаты
        result = {
            isin: merge_bond_data(moex_data.get(isin, {}), ratings.get(isin, {}))
            for isin in (set(moex_data) | set(ratings)) - kval_isins
        }
                
    return result


def get_currency_rate(currency: str) -> float:
    """
    Получает текущий курс валюты в рублях
    
    Args:
        currency (str): Код валюты (EUR, USD, RUB, SUR и т.д.)
        
    Returns:
        float: Курс валюты в рублях. Для рубля возвращает 1.0
    """
    # Нормализуем входную строку
    currency = currency.upper().strip()
    
    # Для рубля сразу возвращаем 1
    if currency in ('RUB', 'SUR', '₽', 'RUR'):
        return 1.0
    
    fallback_rates = {
        'USD': 90.0,
        'EUR': 98.0,
        'CNY': 12.5,
        'AED': 25.0,
        # Можно добавить другие валюты
    }
        
    # Маппинг альтернативных обозначений
    currency_map = {
        'USD': 'USD',
        '$': 'USD',
        'EUR': 'EUR',
        '€': 'EUR',
        'GBP': 'GBP',
        '£': 'GBP',
        'CNY': 'CNY',
        'JPY': 'JPY',
        'AED': 'AED',
        # Можно добавить другие валюты и их альтернативные обозначения
    }
        
    try:
        # Получаем курсы с сайта ЦБ РФ
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', headers=headers)
        response.raise_for_status()
        rates = response.json()
        
        # Получаем стандартное обозначение валюты
        currency_code = currency_map.get(currency, currency)
        
        # Получаем курс из ответа API
        if currency_code in rates['Valute']:
            return rates['Valute'][currency_code]['Value']
            
        raise ValueError(f"Неизвестная валюта: {currency}")
        
    except requests.RequestException as e:
        print(f"Ошибка при получении курсов валют: {e}")
        # В случае ошибки можно использовать резервный источник или закешированные значения
        return fallback_rates.get(currency_map.get(currency, currency), 1.0)
    
    except Exception as e:
        print(f"Неожиданная ошибка при получении курса валюты {currency}: {e}")
        return 1.0

def add_coupons(session, bond, last_payday=None):
    try:
        changed = False
        # Используем переданную сессию
        coupons_response = fetch_sync(session, f'https://admin.fin-plan.org/api/actives/v1/getCoupons?isin={bond.isin}&initialfacevalue=1000')
        bond_response = fetch_sync(session, f'https://admin.fin-plan.org/api/actives/v1/getObligationDetail?isin={bond.isin}&gx_session=undefined')
        
        if not coupons_response or not bond_response:
            return
            
        data = json.loads(coupons_response)['data']
        coupons_data = data.get('COUPONS', [])
        bond_data = json.loads(bond_response)['data']['radar_data']
        if data.get('COUPONS_TOTAL'):
            total = data['COUPONS_TOTAL']['Гашение']
        else:
            total = sum(coup['Гашение'] for coup in data['COUPONS'] if isinstance(coup['Гашение'], (int, float)))
        bond.has_amort = total > 0
        if bond.has_amort:
            changed = True
    except json.decoder.JSONDecodeError:
        db_session.add(ExitBond(isin=bond.isin, name=bond.name, reason='JSONDecodeError'))
        db_session.delete(bond)
        db_session.commit()
        return
    if bond_data['isqualifiedinvestors'] != 0:
        db_session.add(ExitBond(isin=bond.isin, name=bond.name, reason='isqualifiedinvestors'))
        db_session.delete(bond)
        db_session.commit()
        return

    val = bond_data['currency_id']
    if not bond.nominal:
        changed = True
        bond.nominal = bond_data['facevalue'] * get_currency_rate(val)
    if not bond.price and (price := bond_data['lastprice']):
        changed = True
        bond.price = price * get_currency_rate(val)
    
    has_unknown_coups = False
    if len(coupons_data) > 0:
        # если есть таблица с купонами
        coupons_data = [coup for coup in coupons_data if (datetime.strptime(coup['Дата выплаты'], "%d.%m.%Y").date() > datetime.today().date())]
        if not coupons_data:
            db_session.delete(bond)
            db_session.commit()
            return
        for coup in coupons_data:
            payday = datetime.strptime(coup['Дата выплаты'], "%d.%m.%Y").date()
            if last_payday and payday <= last_payday:
                continue
            if bond.oferta and bond.oferta < payday:
                has_unknown_coups = True
                changed = True
                break
            amort = coup['Гашение']
            q = coup['Купоны']
            if q in ("Купон пока не определен", '-'):
                has_unknown_coups = True
                changed = True
                # закончились известные купоны
                break
            try:
                coup = Coupon(isin=bond.isin, coup=float(q) * 0.87, payday=payday, amort=amort, temp=has_unknown_coups)
                db_session.merge(coup)
                db_session.commit()
            except IntegrityError as e:
                db_session.rollback()
                print(f"Failed to insert: {bond.isin}, Error: {e}")
        if bond.unknown_coupons != has_unknown_coups:
            bond.unknown_coupons = has_unknown_coups
            changed = True
    if changed:
        db_session.merge(bond)
        db_session.commit()

def get_bond_data():
    moex_data = asyncio.run(main())
    print("Записываем данные облигаций в базу")
    for isin, data in moex_data.items():
        try:
            bond = Bond(name=data['name'] if data['name'] else None, isin=isin, 
                        price=data['price'], rating=data.get('rating'), 
                        end_date=data['end_date'], 
                        oferta=data['oferta'], 
                        oferta_price=data['oferta_price'],
                        bid=data['bid'], offer=data['offer'], nkd=data['nkd'], nominal=data['nominal'],
                        has_amort=False)
            db_session.merge(bond)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Failed to insert: {isin}, Error: {e}")

def update_coupons():
    print("Обновляем купоны")
    for coupon in db_session.query(Coupon).filter(Coupon.payday < date.today()).all():
        db_session.delete(coupon)
    db_session.commit()
    bonds_with_paydays = (
        db_session.query(
            Bond,
            func.max(Coupon.payday).label('last_payday')
        )
        .outerjoin(Coupon, Bond.isin == Coupon.isin)
        .filter(or_(Bond.unknown_coupons == True, Bond.unknown_coupons.is_(None)))
        .group_by(Bond)
        .all()
    )
    pbar = tqdm(bonds_with_paydays)
    with requests.Session() as session:
        for bond, last_payday in pbar:
            pbar.set_description(bond.name or bond.isin)
            add_coupons(session, bond, last_payday)

def save_results_to_csv(filename='Итоги.csv', limit=50):
    """
    Сохраняет отсортированные результаты расчетов в CSV файл
    
    Args:
        filename (str): Имя файла для сохранения
        limit (int): Количество строк для сохранения
    """
    print("Сохраняем результаты в CSV")
    # Определяем порядок рейтингов
    rating_order = {
        'AAA': 1, 'AA+': 2, 'AA': 3, 'AA-': 4,
        'A+': 5, 'A': 6, 'A-': 7,
        'BBB+': 8, 'BBB': 9, 'BBB-': 10
    }

    # Получаем отсортированные результаты
    results = db_session.query(
        Calc,
        Bond.name,
        Bond.rating,
        Bond.unknown_coupons
    ).join(Bond).order_by(
        case(
            (Calc.yield_iy > 35, 1),
            (Calc.yield_iy.between(30, 35), 2),
            (Calc.yield_iy.between(25, 30), 3),
            (Calc.yield_iy.between(20, 25), 4),
            (Calc.yield_iy.between(15, 20), 5),
            (Calc.yield_iy.between(10, 15), 6),
            else_=7
        ),
        case(
            *[(Bond.rating == k, v) for k, v in rating_order.items()],
            else_=99
        ),
        Calc.yield_iy.desc()
    ).limit(limit).all()

    # Записываем результаты в CSV
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        
        # Записываем заголовки
        headers = [
            'ISIN',
            'Название',
            'Рейтинг',
            'Неизвестные купоны',
            'Цена покупки',
            'Доходность, %',
            'Доходность с учетом инфляции, %',
            'Годовая доходность, %',
            'Годовая доходность с учетом инфляции, %',
            'Срок, лет'
        ]
        writer.writerow(headers)
        
        # Записываем данные
        for calc, name, rating, unknown_coupons in results:
            row = [
                calc.isin,
                name,
                rating,
                '+' if unknown_coupons else '',
                round(calc.purchase_price, 2),
                round(calc.yield_p, 2),
                round(calc.yield_i, 2),
                round(calc.yield_y, 2),
                round(calc.yield_iy, 2),
                round(calc.years, 2)
            ]
            writer.writerow(row)

    print(f"Результаты сохранены в файл {filename}")

if __name__ == '__main__' :
    get_bond_data()
    update_coupons()        
    update_bond_calculations()
    save_results_to_csv()
