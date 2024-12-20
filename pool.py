import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter("ignore", category=ConnectionResetError)

import asyncio
import aiohttp
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup
from collections import Counter
import json
from datetime import datetime, date
import os
import requests
import pandas as pd
from tqdm import trange
import csv
from pathlib import Path
from sqlalchemy import create_engine, case, literal_column, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import or_
from tqdm import tqdm
import aiomoex

import time
from const import no_exist, kval, map_rating
from models import Bond, Coupon, Base, ExitBond
from sqlalchemy.exc import IntegrityError


# engine = create_engine('postgresql+psycopg2://uetlairflow:postgres@77.37.238.118:22018/postgres')
engine = create_engine('sqlite:///pool.db')
if not Path('pool.db').exists():
    Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
db_session = Session()


token_lock = asyncio.Lock()

def flatten_list(nested_list):
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list

async def get_rating():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        global token
        headers = {"Authorization": f"Bearer {token}"}
        url = 'https://oko.grampus-studio.ru/api/bonds/?page=1&page_size=20000&order=desc&order_by=id'
        payload = {
            'loose': True,
            # 'search': isin,
            'status': ["В обращении", "Аннулирован", "Дефолт"]} 
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
                    for row in data['results']:
                        ratings = Counter([k['value'] for k in row['ratings'] if k['value'] not in (None, 'Отозван', 'отозван')])
                        if ratings:
                            row['ratings'] = ratings.most_common(1)[0][0]
                            row.pop('borrower')
                            isin_ratings[row['isin']] = row
                    return isin_ratings
            except (aiohttp.client_exceptions.ClientOSError, aiohttp.client_exceptions.ServerDisconnectedError):
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
        # except aiohttp.ClientConnectorError as e:
        #     print(f"Ошибка подключения: {e}")
        # except aiohttp.ClientError as e:
        #     print(f"Ошибка клиента: {e}")
        # except asyncio.exceptions.CancelledError:
        #     print(f"URL {url} не смог быть обработан")
        except Exception as e:
            pass
            # print(f"Неизвестная ошибка {e}")

        retries += 1
        await asyncio.sleep(1)  # Ожидаем перед повторной попыткой

    # print(f"{url} не смог быть обработан")
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

async def process_site(session, site_url, soup="", params=None):
    if not soup:
        html = await fetch(session, site_url)
        soup = BeautifulSoup(html, "html.parser")
    tags = soup.find_all('table', class_='_hidden')[0].find_all('tr')

    tasks = []

    for tag in tags[1:]:
        def_params = {}
        tds = tag.find_all("td")
        for k, v in params.items():
            value = tds[v].find_all(string=True)
            if value:
                def_params[k] = value[0].string.strip()
        if def_params["Имя"] in no_exist:
            continue
        flag = False
        for i in kval:
            if i in def_params["Имя"]:
                flag = True
                break
        if flag:
            continue
        isin = tds[1].a["href"].split('/')[-2]
        if "ofz" in site_url:
            def_params['Рейтинг'] = 'AAA'
        # elif "subfed" in site_url or "bond" in site_url or def_params["Рейтинг"] in (None, '', '-'):
        #     def_params['Рейтинг'] = await get_rating(isin)
        # if (def_params.get("Рейтинг") or '').lower() in ("аннулирован", "дефолт", "отозван"):
        #     continue
        def_params['Рейтинг, порядок'] = map_rating.get(def_params['Рейтинг'], 0)
        tasks.append((f'https://fin-plan.org/lk/obligations/{isin}/' , def_params))

    print(f"Страница {site_url} закончена")
    return tasks

async def get_pages(session, site_url, params):
    html = await fetch(session, site_url)
    soup = BeautifulSoup(html, "html.parser")
    last_link = soup.find_all("a", class_="pager__link pager__link--arrow")
    coroutines = []
    if not last_link:
        coroutines.append(process_site(session=session, site_url=site_url, soup=soup, params=params))
    else:
        href = last_link[-1]["href"]
        pages = int(href[href.find("page") + 4 : -1])
        coroutines.append(process_site(session=session, site_url=site_url, soup=soup, params=params))
        for i in range(2, pages + 1):
            coroutines.append(
                process_site(
                    session=session,
                    site_url="https://smart-lab.ru" + href.replace(str(pages), str(i)),
                    soup=None, 
                    params=params,
                )
            )

    results = await asyncio.gather(*coroutines)
    return results

def merge_bond_data(moex_data={}, rating_data={}):
    """
    Объединяет данные из двух источников, предпочитая непустые значения
    """
    result = {}
    
    # Базовые поля
    result['isin'] = moex_data.get('ISIN') or rating_data.get('isin')
    result['name'] = moex_data.get('SHORTNAME', None) or rating_data.get('short', None)
    result['full_name'] = rating_data.get('name') if rating_data else None
    
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
    data = {x['ISIN']: x for x in data if any([x["BID"], x["OFFER"], x["MARKETPRICETODAY"]]) and x["FACEUNIT"] == "SUR"}
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
        moex_task = ask_moex(session, xml_url, kwargs)
        ratings_task = get_rating()
        
        # Параллельно получаем список квалифицированных и не доступных облигаций
        kval_isins = {isin[0] for isin in db_session.query(ExitBond.isin).all()}
        
        # Дожидаемся получения данных
        moex_data, ratings = await asyncio.gather(moex_task, ratings_task)
        
        # Фильтруем результаты
        result = {
            isin: merge_bond_data(moex_data.get(isin, {}), ratings.get(isin, {}))
            for isin in (set(moex_data) | set(ratings)) - kval_isins
        }
                
    return result

inf4day = 1 - 0.15 / 365  # коэффициент обесценивания денег за счёт инфляции
of_inf4day = 1 - 0.09 / 365  # официальная инфляция
gcurve7 = 0.118
stavka = 0.21
finished = []
big_nom = []
def extr(obl, text, tagname="p"):
    """
    Извлечение значения по тексту
    """
    fnd = obl.find_all(lambda tag: text in tag.text and tag.name == tagname)
    if not fnd:
        return None
    return fnd[0].text.strip()

def my_func(url, params):
    """
    Расчёт прибыли одной облигации
    """
    html = requests.get(url).text
    obl = BeautifulSoup(html, "html.parser")
    name = params.get("Имя")
    rating = params.get("Рейтинг")
    if len(params["Погашение"]) == 10:
        form = "%d.%m.%Y"
    else:
        form = "%d.%m.%y"
    days = (datetime.strptime(params["Погашение"], form) - datetime.now()).days
    if days < 2:
        return {}
    years = days / 365
    if years == 0:
        # не учитывать закончившиеся
        finished.append(name)
        return {}
    nom = extr(obl, "Номинал: ").split(' ')[1]
    if '$' in nom:
        # print(f'проверить доступность {name} в $')
        return {}
    elif 'EUR' in nom:
        # print(f'проверить доступность {name} в EUR')
        return {}
    else:
        nom = float(nom)
    if nom > 10000:
        big_nom.append(name)
        return {}
    if params.get('Цена', '-') == '-':
        cost_p = extr(obl, "Текущая цена: ")
        if cost_p:
            cost_p = cost_p[:-1]
        if not cost_p:
            return {}
        cost_p = float(cost_p)
    else:
        cost_p = float(params['Цена'])
    oferta = None
    if params.get("Оферта") and params["Оферта"] != '-':
        oferta = datetime.strptime(params['Оферта'], form).date()
        if oferta <= datetime.now().date():
            oferta = None
        else:
            days = (oferta - datetime.today().date()).days
            years = days / 365

    pk = obl.find_all(lambda tag: tag.name == 'p' and 'Формула расчета купона' in tag.text)
    dop_stavka = 0
    if pk:
        dop_stavka = float(pk[0].text.split('%')[0].split(' ')[-1]) if '%' in pk[0].text else 0
        params["Переменный купон"] = 'ПК'
    nkd = float(params['НКД, руб'].replace('-', '0'))
    # стоимость с учётом НКД и комиссией
    cost = (cost_p / 100 * nom + nkd) * 1.0006
    profit = 0  # прибыль
    profit_in = 0  # прибыль с учётом инфляции
    tab = obl.findAll("tbody")
    if len(tab) > 0:
        # если есть таблица с купонами
        tab = tab[0]
        if params.get("Переменный купон") in ('ПК', 'ИН'):
            coups = tab.find_all('tr', class_='coupon_table_row')
            freq = len(coups) / (datetime.strptime(coups[-1].td.string, "%d.%m.%Y").date() - datetime.strptime(coups[0].td.string, "%d.%m.%Y").date()).days * 365
        cur_line = tab.findAll(class_="green")[0]
        for coup in [cur_line] + list(cur_line.next_siblings):
            if coup == '\n' or not coup.get('class'):
                continue
            tds = coup.find_all('td')
            n = datetime.strptime(tds[0].string, "%d.%m.%Y").date()
            if oferta and oferta < n:
                break
            delta = (n - datetime.today().date()).days
            if delta < 3:
                # если купон выплачивается послезавтра и раньше, не считается
                continue
            if params.get("Переменный купон") == 'ПК':
                q = nom * (stavka + dop_stavka) / freq
            else:
                q = tds[2].string
            if params.get("Переменный купон") == 'ИН':
                q = nom * 0.025 / freq
            if q == "Купон пока не определен":
                days = delta    # не учитывать закончившиеся
                years = delta / 365
                oferta = n
                # закончились известные купоны
                break
            gash = float(tds[3].string)
            inc = float(q) * 0.87 + gash  # купон за вычетом налогов с погашением
            nom -= gash
            profit += inc
            # защита от официальной инфляции
            profit_in += inc * inf4day**delta / (of_inf4day**delta if params.get("Переменный купон") == 'ИН' else 1)
    nom = min(cost, nom) if oferta else nom
    nom = nom / (of_inf4day**delta) if params.get("Переменный купон") == 'ИН' else nom
    perc = ((profit + nom) / cost - 1) * 100  # общая прибыль
    perc_g = perc / years  # прибыль в год
    perc_i = (
            (profit_in + nom * inf4day**days) / cost - 1
        ) * 100  # общая прибыль с учётом инфляции
    perc_in = perc_i / years  # прибыль в год с учётом инфляции
    return {
        "Облигация": name,
        "Прибыль, %": round(perc, 3),
        "Прибыль с учётом инфляции, %": round(perc_i, 3),
        "Годовая прибыль, %": round(perc_g, 3),
        "Годовая прибыль с учётом инфляции, %": round(perc_in, 3),
        "Дата погашения": datetime.strptime(params["Погашение"], form),
        "Рейтинг": rating,
        'Оферта': oferta,
        "Цена, %": cost_p,
        'Рейтинг, порядок': params['Рейтинг, порядок'],
    }

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
        # Используем переданную сессию
        coupons_response = fetch_sync(session, f'https://admin.fin-plan.org/api/actives/v1/getCoupons?isin={bond.isin}&initialfacevalue=1000')
        bond_response = fetch_sync(session, f'https://admin.fin-plan.org/api/actives/v1/getObligationDetail?isin={bond.isin}&gx_session=undefined')
        
        if not coupons_response or not bond_response:
            return
            
        coupons_data = json.loads(coupons_response)['data'].get('COUPONS', [])
        bond_data = json.loads(bond_response)['data']['radar_data']
    except json.decoder.JSONDecodeError:
        db_session.add(ExitBond(isin=bond.isin, name=bond.name, full_name=bond.full_name, reason='JSONDecodeError'))
        db_session.delete(bond)
        db_session.commit()
        return
    if bond_data['isqualifiedinvestors'] != 0:
        db_session.add(ExitBond(isin=bond.isin, name=bond.name, full_name=bond.full_name, reason='isqualifiedinvestors'))
        db_session.delete(bond)
        db_session.commit()
        return
    changed = False
    val = bond_data['currency_id']
    if not bond.nominal:
        changed = True
        bond.nominal = bond_data['facevalue'] * get_currency_rate(val)
    if not bond.price and (price := bond_data['lastprice']):
        changed = True
        bond.price = price * get_currency_rate(val)
    if changed:
        db_session.merge(bond)
        db_session.commit()
    
    has_unknown_coups = False
    # pk = obl.find_all(lambda tag: tag.name == 'p' and 'Формула расчета купона' in tag.text)
    dop_stavka = 0
    # if pk:
    #     bond.floating = 'ПК'
    #     changed = True
    if len(coupons_data) > 0:
        # если есть таблица с купонами
        coupons_data = [coup for coup in coupons_data if (datetime.strptime(coup['Дата выплаты'], "%d.%m.%Y").date() > datetime.today().date())]
        if not coupons_data:
            # db_session.delete(bond)
            # db_session.commit()
            return
        # if bond.floating in ('ПК', 'ИН'):
        #     coups = tab.find_all('tr', class_='coupon_table_row')
        #     freq = len(coups) / (datetime.strptime(coups[-1].td.string, "%d.%m.%Y").date() - datetime.strptime(coups[0].td.string, "%d.%m.%Y").date()).days * 365
        for coup in coupons_data:
            payday = datetime.strptime(coup['Дата выплаты'], "%d.%m.%Y").date()
            if last_payday and payday <= last_payday:
                continue
            if bond.oferta and bond.oferta < payday:
                has_unknown_coups = True
                break
            amort = coup['Гашение']
            q = coup['Купоны']
            if q in ("Купон пока не определен", '-'):
                # if bond.floating == 'ПК':
                #     if not dop_stavka and pk:
                #         dop_stavka = float(pk[-1].text.split('%')[0].split(' ')[-1]) if '%' in pk[-1].text else 0
                #     q = bond.nominal * (stavka + dop_stavka) / freq
                # if bond.floating == 'ИН':
                #     q = bond.nominal * 0.025 / freq
                # else:
                has_unknown_coups = True
                # закончились известные купоны
                break
            try:
                coup = Coupon(isin=bond.isin, coup=float(q) * 0.87, payday=payday, amort=amort, temp=has_unknown_coups)
                db_session.merge(coup)
                db_session.commit()
            except IntegrityError as e:
                db_session.rollback()
                print(f"Failed to insert: {bond.isin}, Error: {e}")
        bond.unknown_coupons = has_unknown_coups
        db_session.merge(bond)
        db_session.commit()

if __name__ == '__main__' :
    moex_data = asyncio.run(main())
    for isin, data in moex_data.items():
        try:
            bond = Bond(name=data['name'] if data['name'] else None, isin=isin, 
                        price=data['price'], rating=data.get('rating'), 
                        end_date=data['end_date'], 
                        oferta=data['oferta'], 
                        oferta_price=data['oferta_price'],
                        bid=data['bid'], offer=data['offer'], nkd=data['nkd'], nominal=data['nominal'],)
            db_session.merge(bond)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Failed to insert: {isin}, Error: {e}")
            
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

