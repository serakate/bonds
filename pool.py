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
from multiprocessing import Lock
import csv



token_lock = asyncio.Lock()
no_exist = [
    "ОткрФКББ03",
    "МФК ЦФПО02",
    "МФК ЦФПО01",
    "МаниМен 03",
    "Займер 01",
    "АйДиКоле01",
    "МаниМен 02",
    "ОткрБРСО3",
    "ОткрБРСО5",
    "ВЭББНКР 01",
    "Займер 02",
    "Маныч02",
    "Брус 1P02",
    "Займер 03",
    "Kviku1P1",
    "AAG-01",
    "ГПБ-КИ-02",
    "СФОВТБИП03",
    "ОткрБРСО6",
    "СФОВТБИП02",
    "АйДиКоле02",
    "ВЭББНКР 02",
    "АСПЭКДом01",
    "ЛаймЗайм01",
    "КарМани 01",
    "ОткрБРСО4",
]
kval = ["МежИнБ",
        "Самолет1P4",
    "Феррони",
    "ОйлРесур0",
    "МГКЛ 1P",
    "АйДиКоле03",
    "ВТБ Б1-",
    "BCS",
    "СберИОС",
    "МКБ П0",
    "ВТБ Б-1",
    "ОткрФКБИО",
    "МКБ П",
    "ТинькоффИ",
    "ОткрФКИОП",
    "СибЭнМаш",
    "ВЭББНКР ",
    "ОткрФКИО13",
    "ОткрБРСО7",
    "БКСБ1Р-01",
    "ЕАБР 1Р-04",
    "ГПБ002P-12",
    "МежИнБанк4",
    "ЛаймЗайм02",
    "ОткрБРСО9",
    "ОткрБРСО8",
    "МежИнБ01P5",
    "МежИнБ01P5",
    "МежИнБ01P1",
    "ОткрБРСО12",
    "ОткрБРСО11",
    "ОткрФКИО12",
    "Страна 01",
    "МГКЛ 1P3",
    "АйДиКоле03",
    "ВТБ Б1-",
    "BCS",
    "СберИОС",
    "МКБ П0",
    "ВТБ Б-1",
    "ОткрФКБИО",
    "МКБ П",
    "ТинькоффИ",
    "ОткрФКИОП",
    "СибЭнМаш",
    "ВЭББНКР ",
    "ОткрФКИО13",
    "ОткрБРСО7",
    "БКСБ1Р-01",
    "ЕАБР 1Р-04",
    "ГПБ002P-12",
    "МежИнБанк4",
    "ЛаймЗайм02",
    "ОткрБРСО9",
    "ОткрБРСО8",
    "МежИнБ01P5",
    "МежИнБ01P5",
    "МежИнБ01P1",
    "ОткрБРСО12",
    "ОткрБРСО11",
    "ОткрФКИО12",
    "Страна 01",
]
map_rating = {'AAA': 17, 'AAA-': 16, 'AA+': 15, 'AA': 14, 'AA-': 13, 'A+': 12, 'A': 11, 'A-': 10, 'BBB+': 9, 'BBB': 8, 'BBB-': 7, 'BB+': 6, 'BB': 5, 'BB-': 4, 'B+': 3, 'B': 2, 'B-': 1, 'CCC': 0}

def flatten_list(nested_list):
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list
async def get_rating(isin):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        global token
        headers = {"Authorization": f"Bearer {token}"}
        url = 'https://oko.grampus-studio.ru/api/bonds/?page=1&page_size=200&order=desc&order_by=name'
        payload = {
            'loose': True,
            'search': isin,
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
                    ratings = Counter([k['value'] for k in data['results'][0]['ratings'] if k['value'] not in (None, 'Отозван', 'отозван')])
                    if ratings:
                        return ratings.most_common(1)[0][0]
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
        elif "subfed" in site_url or "bond" in site_url or def_params["Рейтинг"] in (None, '', '-'):
            def_params['Рейтинг'] = await get_rating(isin)
        if (def_params.get("Рейтинг") or '').lower() in ("аннулирован", "дефолт", "отозван"):
            continue
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
    site_urls = [
        ("https://smart-lab.ru/q/bonds",{"Имя": 1, "Рейтинг": 7, "НКД, руб": 11, "Цена": 13, "Погашение": 16, "Оферта": 17}),
        ("https://smart-lab.ru/q/ofz",{"Имя": 1, "Переменный купон": 6, "НКД, руб": 14, "Цена": 9, "Погашение": 3}),
        ("https://smart-lab.ru/q/subfed",{"Имя": 1, "НКД, руб": 13, "Цена": 8, "Погашение": 3, "Оферта": 16}),
    ]

    async with aiohttp.ClientSession() as session:
        site_tasks = []

        for site_url, params in site_urls:
            site_task = asyncio.ensure_future(get_pages(session, site_url, params))
            site_tasks.append(site_task)

        result = await asyncio.gather(*site_tasks)
    return result

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

if __name__ == '__main__' :
    file_path = "urls.json"
    sites = None
    dd = None
    if os.path.exists(file_path):
        dt = os.path.getmtime(file_path)
        dd = date.fromtimestamp(dt)
    if dd != date.today():
        loop = asyncio.get_event_loop()
        sites = loop.run_until_complete(main())
        sites = flatten_list(sites)
        with open(file_path, "w") as f:
            json.dump(sites, f)
    if sites is None:
        sites = json.load(open(file_path, "r"))

    
    with open('Итоги.csv', 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';', separator=',')
        writer.writerow([
            "Облигация",
            "Прибыль, %",
            "Прибыль с учётом инфляции, %",
            "Годовая прибыль, %",
            "Годовая прибыль с учётом инфляции, %",
            "Дата погашения",
            "Рейтинг",
            "Оферта",
            "Цена, %",
            "Рейтинг, порядок",
        ])

    rows = []
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(my_func, site_url, params) for site_url, params in sites]
        with trange(len(futures)) as t:
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                rows.append(result)
                t.set_description(result["Облигация"])
                t.update(1)

    df = pd.DataFrame(rows)
    df.to_csv(path_or_buf="Итоги.csv", sep=",", mode="a+", header=False)
        
