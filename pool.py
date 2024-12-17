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

from const import no_exist, kval, map_rating
from models import Bond, Coupon, Base


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
        
        moex_task = ask_moex(session, xml_url, kwargs)

        ratings_task = get_rating()

        moex_data, ratings = await asyncio.gather(moex_task, ratings_task)
        for k, v in ratings.items():
            if k in moex_data:
                moex_data[k]['rating'] = v
            else:
                moex_data[k] = {'SHORTNAME': v['short'], 'ACCRUDIENT': v['nkd'], 'OFFERDATE': v['offer_date'], 'BUYBACKPRICE': v['buyback_price'], 'LOTVALUE': v['dolg'], 'MATDATE': v['maturity_date'], 'MARKETPRICETODAY': v['last_price'], 'rating': v['ratings']}
    return moex_data

inf4day = 1 - 0.13 / 365  # коэффициент обесценивания денег за счёт инфляции
of_inf4day = 1 - 0.06 / 365  # официальная инфляция
gcurve7 = 0.118
stavka = 0.14
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
dol = 90
eur = 100
aed = 25

def add_coupons(bond, last_payday=None):
    html = requests.get(bond.url).text
    obl = BeautifulSoup(html, "html.parser")
    changed = False
    if not bond.nominal:
        changed = True
        nom = extr(obl, "Номинал: ").split(' ')[1]
        nom = nom.replace('KGS', '') if nom else nom
        if '$' in nom:
            nom = nom.replace('$', '')
            nom = float(nom) * dol
        elif 'EUR' in nom:
            nom = nom.replace('EUR', '')
            nom = float(nom) * eur
        elif 'AED' in nom:
            nom = nom.replace('AED', '')
            nom = float(nom) * aed
        else:
            bond.nominal = float(nom)
    if not bond.price:
        cost_p = extr(obl, "Текущая цена: ").replace('Текущая цена: ', '')
        if cost_p:
            cost_p = cost_p.replace('Текущая цена: ', '')[:-1]
            bond.price = cost_p
            changed = True
    if changed:
        db_session.merge(bond)
        db_session.commit()
    
    has_unknown_coups = False
    pk = obl.find_all(lambda tag: tag.name == 'p' and 'Формула расчета купона' in tag.text)
    dop_stavka = 0
    if pk:
        bond.floating = 'ПК'
        changed = True
    tab = obl.findAll("tbody")
    if len(tab) > 0:
        # если есть таблица с купонами
        tab = tab[0]
        cur_line = tab.findAll(class_="green")
        if not cur_line:
            db_session.delete(bond)
            db_session.commit()
            return
        cur_line = cur_line[0]
        if bond.floating in ('ПК', 'ИН'):
            coups = tab.find_all('tr', class_='coupon_table_row')
            freq = len(coups) / (datetime.strptime(coups[-1].td.string, "%d.%m.%Y").date() - datetime.strptime(coups[0].td.string, "%d.%m.%Y").date()).days * 365
        for coup in [cur_line] + list(cur_line.next_siblings):
            if coup == '\n' or not coup.get('class'):
                continue
            tds = coup.find_all('td')
            payday = datetime.strptime(tds[0].string, "%d.%m.%Y").date()
            if last_payday and payday <= last_payday:
                continue
            if bond.oferta and bond.oferta < payday:
                has_unknown_coups = True
                break
            amort = float(tds[3].string)
            q = tds[2].string
            if q in ("Купон пока не определен", '-'):
                if bond.floating == 'ПК':
                    if not dop_stavka and pk:
                        dop_stavka = float(pk[-1].text.split('%')[0].split(' ')[-1]) if '%' in pk[-1].text else 0
                    q = bond.nominal * (stavka + dop_stavka) / freq
                if bond.floating == 'ИН':
                    q = bond.nominal * 0.025 / freq
                else:
                    has_unknown_coups = True
                    # закончились известные купоны
                    break
            coup = Coupon(isin=bond.isin, coup=float(q) * 0.87, payday=payday, amort=amort, temp=has_unknown_coups)
            db_session.merge(coup)
            db_session.commit()
        bond.unknown_coupons = has_unknown_coups
        db_session.merge(bond)
        db_session.commit()

if __name__ == '__main__' :
    moex_data = asyncio.run(main())
    for isin, data in moex_data.items():
        try:
            format = "%Y-%m-%d"
            bond = Bond(name=data['SHORTNAME'], url=f'https://fin-plan.org/lk/obligations/{isin}', isin=isin, 
                        price=data['MARKETPRICETODAY'], rating='AAA' if 'ОФЗ' in data['SHORTNAME'] else data.get('rating'), 
                        end_date=datetime.strptime(data['MATDATE'], format), 
                        oferta=datetime.strptime(data['OFFERDATE'], format) if data.get('Оферта', '-') != '-' else None, 
                        oferta_price=data['BUYBACKPRICE'],
                        bid=data['BID'], offer=data['OFFER'], nkd=data['ACCRUEDINT'], nominal=data['LOTVALUE'],)
            db_session.merge(bond)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Failed to insert: {isin}, Error: {e}")
            
    for coupon in db_session.query(Coupon).filter(Coupon.payday < date.today()).all():
        db_session.delete(coupon)
    db_session.commit()
    no_coups = db_session.query(Bond).filter(Bond.unknown_coupons.in_([True, None])).all()
    pbar = tqdm(no_coups)
    for bond in pbar:
        last_payday = db_session.query(func.max(Coupon.payday)).filter(Coupon.bond == bond).scalar()
        pbar.set_description(bond.name)
        add_coupons(bond, last_payday)
