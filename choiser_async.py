import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import re

inf4day = 1 - 0.07 / 365  # коэффициент обесценивания денег за счёт инфляции
nonliqvid = []
cancel = []
no_cost = []
finished = []
no_date = []
last_deal = []
big_nom = []
kval = [
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
kval1 = []
no_exist = [
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
exc = [
    "СолЛиз",
    "АйДиЭф",
    "ПионЛиз",
    "РоялКап",
    "СТТ ",
    "Финанс-М",
    "ПЕРЕСВ",
    "СПбТел ",
    "ОткрХОЛ",
    "БДеньги",
    "ЭлщитСт",
    "ЭконЛиз",
    "ЭБИС ",
    "ЮниМетр",
    "РКССочи",
    "Урожай",
    "ДрктЛиз",
    "ДЭНИКОЛБ",
    "Каскад",
    "ВейлФин",
    "ОВК Фин",
    "ДДёнер",
    "АрчерФин",
    "Пропфн",
    "ОВК Фин",
    "ЭНКО ",
    "МОСТРЕСТ ",
    "Труд ",
    "КПокров",
    "АПРИФП ",
    "ГрупПро",
    "ЧЗПСНП ",
    "ГарИнв",
    "ЗавдКЭС",
    "Шевченк",
    "Полипл",
    "Победа",
    "Держава",
    "ТАЛАНФБ",
    "ЛТрейд",
    "ИнДевел",
    "СОкс",
    "ХКФин",
    "РКС",
    "ОРГрупп",
    "РОСНАН",
    "ЛИТАНА ",
    "ТЕХЛиз ",
    "ТДСинтБО",
    "Калсб ",
    "ПЮДМ ",
    "ОхтаГр ",
    "СЭЗ ",
    "Аэрфью",
    "Оптима",
    "БЭЛТИ ",
    "Калита",
    "ЛЕГЕНДА",
    "НафттрнБО",
    "КарМаниБ",
    "КЛС БО",
    "МоторТ",
    "NexTouch",
    "ОхтаГр",
    "СНХТ ",
    "ГЛАВТОРГ",
    "Пропфин",
    "ТРИНФ Х ",
    "РегПрод",
    "СПМК ",
]
kval = [
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
    t = t[0]
    if a:
        t = t.parent.parent
    if title:
        return t.contents[3].string.strip()
    return t.next_sibling.next_sibling.string.strip()


def my_func(obl):
    """
    Расчёт прибыли одной облигации
    """
    name = extr(obl, "Название")
    if nonl(obl):
        nonliqvid.append(name)
        return {}
    kval = extr(obl, "Только для квалов?")
    if kval != "Нет":
        return {}
    if name in no_exist:
        return {}
    years = float(extr(obl, "Лет до погашения"))
    days = int(years * 365)
    if years == 0:
        # не учитывать закончившиеся
        finished.append(name)
        return {}
    nom = float(extr(obl, "Номинал"))
    if nom > 1500:
        big_nom.append(name)
        return {}
    cost_p = float(extr(obl, "Котировка облигации, %")[:-1])

    nkd = float(extr(obl, "Накопленный купонный доход", title=True).split()[0])
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
            delta = (n - datetime.today().date()).days
            if delta < 3:
                # если купон выплачивается послезавтра и раньше, не считается
                continue
            q = coup.contents[5].string
            if q == "—":
                # закончились известные купоны
                break
            inc = float(q) * 0.87  # купон за вычетом налогов
            profit += inc
            profit_in += inc * inf4day**delta
    perc = ((profit + nom) / cost - 1) * 100  # общая прибыль
    perc_g = perc / years  # прибыль в год
    if "ОФЗ-ИН" in name:
        perc_i = ((profit_in + nom) / cost - 1) * 100  # офз с защитой от инфляции
    else:
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
        "Дата погашения": datetime.now() + timedelta(days=days),
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
        except aiohttp.ClientConnectorError as e:
            print(f"Ошибка подключения: {e}")
        except aiohttp.ClientError as e:
            print(f"Ошибка клиента: {e}")
        except asyncio.exceptions.CancelledError:
            print(f"URL {url} не смог быть обработан")
        except Exception as e:
            print(f"Неизвестная ошибка {e}")

        retries += 1
        await asyncio.sleep(1)  # Ожидаем перед повторной попыткой

    return None


async def process_tag(
    session,
    tag,
):
    link = tag["href"]

    url = "https://smart-lab.ru" + link
    html = await fetch(session, url)
    if html is None:
        print(f"Не удалось получить {url}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    result = my_func(soup)

    return result


async def process_site(session, site_url, page, soup=""):
    if not soup:
        html = await fetch(session, site_url)
        soup = BeautifulSoup(html, "html.parser")
    tags = soup.find_all("td", class_="trades-table__name")

    tasks = []

    for tag in tags:
        name = tag.string
        if name in kval:
            continue
        f_pass = False
        for e in exc:
            if e in name:
                flag = 1
                cancel.append(name)
                f_pass = True
                break
        if f_pass:
            continue

        task = asyncio.ensure_future(process_tag(session, tag.a))
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


async def get_pages(session, site_url):
    html = await fetch(session, site_url)
    soup = BeautifulSoup(html, "html.parser")
    last_link = soup.find_all("a", class_="pager__link pager__link--arrow")
    coroutines = []
    if not last_link:
        coroutines.append(process_site(session, site_url, 1, soup=soup))
    else:
        href = last_link[-1]["href"]
        pages = int(href[-2])
        coroutines.append(process_site(session, site_url, 1, soup=soup))
        for i in range(2, pages + 1):
            coroutines.append(
                process_site(
                    session,
                    "https://smart-lab.ru" + href.replace(str(pages), str(i)),
                    i,
                )
            )

    await asyncio.gather(*coroutines)


async def main():
    site_urls = [
        "https://smart-lab.ru/q/bonds",
        "https://smart-lab.ru/q/ofz",
        "https://smart-lab.ru/q/subfed",
    ]

    async with aiohttp.ClientSession() as session:
        site_tasks = []

        for site_url in site_urls:
            site_task = asyncio.ensure_future(get_pages(session, site_url))
            site_tasks.append(site_task)

        await asyncio.gather(*site_tasks)


df = pd.DataFrame(
    columns=[
        "Облигация",
        "Прибыль, %",
        "Прибыль с учётом инфляции, %",
        "Годовая прибыль, %",
        "Годовая прибыль с учётом инфляции, %",
        "Дата погашения",
    ]
)
df.to_csv(path_or_buf="Итоги.csv", sep=",")
loop = asyncio.get_event_loop()
# loop.set_debug(True)
loop.run_until_complete(main())
