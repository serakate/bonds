import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import re
from collections import Counter

token = None
token_lock = asyncio.Lock()
stavka = 0.14

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

async def get_rating(isin):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        global token
        headers = {"Authorization": f"Bearer {token}"}
        url = 'https://oko.grampus-studio.ru/api/bonds/?page=1&page_size=200&order=desc&order_by=name'
        payload = {
            'loose': True,
            'search': isin,
            'status': ["В обращении", "Аннулирован", "Дефолт"]} 
        try:
            async with session.post(url, json=payload, headers=headers) as response:
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

inf4day = 1 - 0.13 / 365  # коэффициент обесценивания денег за счёт инфляции
of_inf4day = 1 - 0.06 / 365
nonliqvid = []
cancel = []
no_cost = []
finished = []
no_date = []
last_deal = []
big_nom = []
kval1 = []
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
# exc = [
#     "ВсИнстрБ",
#     "ЛКМБ-РТ1P1",
#     "ВЭБлизБ05",
#     "СолЛиз",
#     "АйДиЭф",
#     "ПионЛиз",
#     "РоялКап",
#     "СТТ ",
#     "Финанс-М",
#     "ПЕРЕСВ",
#     "СПбТел ",
#     "ОткрХОЛ",
#     "БДеньги",
#     "ЭлщитСт",
#     "ЭконЛиз",
#     "ЭБИС ",
#     "ЮниМетр",
#     "РКССочи",
#     "Урожай",
#     "ДрктЛиз",
#     "ДЭНИКОЛБ",
#     "Каскад",
#     "ВейлФин",
#     "ОВК Фин",
#     "ДДёнер",
#     "АрчерФин",
#     "Пропфн",
#     "ОВК Фин",
#     "ЭНКО ",
#     "МОСТРЕСТ ",
#     "Труд ",
#     "КПокров",
#     "АПРИФП ",
#     "ГрупПро",
#     "ЧЗПСНП ",
#     "ГарИнв",
#     "ЗавдКЭС",
#     "Шевченк",
#     "Полипл",
#     "Победа",
#     "Держава",
#     "ТАЛАНФБ",
#     "ЛТрейд",
#     "ИнДевел",
#     "СОкс",
#     "ХКФин",
#     "РКС",
#     "ОРГрупп",
#     "РОСНАН",
#     "ЛИТАНА ",
#     "ТЕХЛиз ",
#     "ТДСинтБО",
#     "Калсб ",
#     "ПЮДМ ",
#     "ОхтаГр ",
#     "СЭЗ ",
#     "Аэрфью",
#     "Оптима",
#     "БЭЛТИ ",
#     "Калита",
#     "ЛЕГЕНДА",
#     "НафттрнБО",
#     "КарМаниБ",
#     "КЛС БО",
#     "МоторТ",
#     "NexTouch",
#     "ОхтаГр",
#     "СНХТ ",
#     "ГЛАВТОРГ",
#     "Пропфин",
#     "ТРИНФ Х ",
#     "РегПрод",
#     "СПМК ",
# ]
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


def flatten_list(nested_list):
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list


def nonl(obl):
    """Не берем бумаги, сделки по которым были более 3 дней назад"""
    target = obl.findAll("script")
    for s in target:
        if s.string is None:
            continue
        match = re.search(r"var aTradeResultsData = {(.+?)}", s.string)
        if match is None:
            continue
        match = "{" + match.group(1) + "}"
        d = json.loads(match)
        if not len(d["dates"]):
            return True
        return (datetime.now() - datetime.strptime(d["dates"][-1], "%d.%m.%Y")).days > 3


def extr(obl, text, a=False, title=False):
    """
    Извлечение значения по тексту
    """
    if title:
        t = obl.findAll("div", title=re.compile(text))
    else:
        t = obl.findAll("div", string=re.compile(text))
    if not t:
        return None
    t = t[0]
    if a:
        t = t.parent.parent
    if title:
        return t.contents[3].string.strip()
    return t.next_sibling.next_sibling.findAll(string=True)[0].string.strip()
    # return t.next_sibling.next_sibling.string.strip()


def my_func(obl, params):
    """
    Расчёт прибыли одной облигации
    """
    name = params.get("Имя", extr(obl, "Название"))
    # if nonl(obl):
    #     nonliqvid.append(name)
    #     return {}
    kval = extr(obl, "Только для квалов?")
    if kval != "Нет":
        return {}
    rating = params.get("Рейтинг", extr(obl, "Кредитный рейтинг"))
    # if name in no_exist:
    #     return {}
    if len(params["Погашение"]) == 10:
        form = "%d.%m.%Y"
    else:
        form = "%d.%m.%y"
    days = (datetime.strptime(params["Погашение"], form) - datetime.now()).days
    if days < 2:
        return {}
    years = days / 365
    # years = float(extr(obl, "Лет до погашения"))
    # days = int(years * 365)
    if years == 0:
        # не учитывать закончившиеся
        finished.append(name)
        return {}
    nom = float(extr(obl, "Номинал"))
    if nom > 10000:
        big_nom.append(name)
        return {}
    if params.get('Цена', '-') == '-':
        cost_p = extr(obl, "Котировка облигации, %")[:-1]
        if cost_p == '':
            return {}
        cost_p = float(cost_p)
    else:
        cost_p = float(params['Цена'])
    # cost_p = float(extr(obl, "Котировка облигации, %")[:-1])
    oferta = None
    if params.get("Оферта") and params["Оферта"] != '-':
        oferta = datetime.strptime(params['Оферта'], form).date()
        if oferta <= datetime.now().date():
            oferta = None
        else:
            days = (oferta - datetime.today().date()).days
            years = days / 365
    if params.get("Переменный купон") == 'ПК':
        freq = float(extr(obl, "Частота купона, раз в год"))

    nkd = float(params['НКД, руб'].replace('-', '0'))
    # nkd = float(extr(obl, "Накопленный купонный доход", title=True).split()[0])
    # стоимость с учётом НКД и комиссией
    cost = (cost_p / 100 * nom + nkd) * 1.0006
    profit = 0  # прибыль
    profit_in = 0  # прибыль с учётом инфляции
    tab = obl.findAll("tbody")
    if len(tab) > 0:
        # если есть таблица с купонами
        tab = tab[0]
        for coup in tab.findAll("tr"):
            n = datetime.strptime(coup.contents[3].string, "%d-%m-%Y").date()
            if oferta and oferta < n:
                break
            delta = (n - datetime.today().date()).days
            if delta < 3:
                # если купон выплачивается послезавтра и раньше, не считается
                continue
            if params.get("Переменный купон") == 'ПК':
                q = nom * stavka / freq
            else:
                q = coup.contents[5].string
            if q == "—":
                days = delta    # не учитывать закончившиеся
                years = delta / 365
                oferta = n
                # закончились известные купоны
                break
            inc = float(q) * 0.87  # купон за вычетом налогов
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

    print(f"{url} не смог быть обработан")
    return None


async def process_tag(
    session,
    tag,
    params
):
    link = tag["href"]

    url = "https://smart-lab.ru" + link
    html = await fetch(session, url)
    if html is None:
        print(f"Не удалось получить {url}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    result = my_func(soup, params)

    return result

ended_lock = asyncio.Lock()
ended = set()
map_rating = {'AAA': 17, 'AAA-': 16, 'AA+': 15, 'AA': 14, 'AA-': 13, 'A+': 12, 'A': 11, 'A-': 10, 'BBB+': 9, 'BBB': 8, 'BBB-': 7, 'BB+': 6, 'BB': 5, 'BB-': 4, 'B+': 3, 'B': 2, 'B-': 1, 'CCC': 0}
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
        if def_params["Имя"] not in ended:
            async with ended_lock:
                ended.add(def_params["Имя"])
        else:
            continue
        if "ofz" in site_url:
            def_params['Рейтинг'] = 'AAA'
        elif "subfed" in site_url or def_params["Рейтинг"] in (None, '', '-'):
            isin = tds[1].a["href"].split('/')[-2]
            def_params['Рейтинг'] = await get_rating(isin)
        if (def_params.get("Рейтинг") or '').lower() in ("аннулирован", "дефолт", "отозван"):
            continue
        def_params['Рейтинг, порядок'] = map_rating.get(def_params['Рейтинг'], 0)
        task = asyncio.ensure_future(process_tag(session, tds[1].a, def_params))
        if task == {}:
            print(f"Бумага {tag.text} не найдена")
        tasks.append(task)

    site_results = await asyncio.gather(*tasks)

    df = pd.DataFrame()
    for result in flatten_list(site_results):
        if result:
            df = df.append(result, ignore_index=True)
    df.to_csv(path_or_buf="Итоги.csv", sep=",", mode="a+", header=False)

    print(f"Страница {site_url} закончена")


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

    await asyncio.gather(*coroutines)


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

        await asyncio.gather(*site_tasks)
    df = pd.read_csv("Итоги.csv", sep=",")
    df.sort_values(["Рейтинг, порядок", "Годовая прибыль с учётом инфляции, %"], ascending=[False, False], inplace=True)
    # df.drop("Рейтинг, порядок", axis=1, inplace=True)
    df.to_csv(path_or_buf="Итоги.csv", sep=",", index=False)


df = pd.DataFrame(
    columns=[
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
    ]
)
df.to_csv(path_or_buf="Итоги.csv", sep=",")
loop = asyncio.get_event_loop()
# loop.set_debug(True)
loop.run_until_complete(main())
df = pd.read_csv("Итоги.csv", sep=",")
max_profit = df.groupby("Рейтинг, порядок")["Рейтинг", "Годовая прибыль с учётом инфляции, %"].max()
print(max_profit)

# Проверить, нужны ли функции
# future = get_running_loop().create_future()
# create_task()
# future.set_result()
# wait(FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED)
# as_completed()
# current_task()
# all_tasks()
# get_running_loop().run_in_executor()
