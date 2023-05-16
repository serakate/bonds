import asyncio
import datetime
import json
import re
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

inf4day = 1 - 0.05 / 365  # коэффициент обесценивания денег за счёт инфляции
# с плохим кредитным рейтингом
exc = ['СолЛиз', "АйДиЭф", "ПионЛиз", "РоялКап", "СТТ ", "Финанс-М", "ПЕРЕСВ", "СПбТел ",
       "ОткрХОЛ", "БДеньги", "ЭлщитСт", "ЭконЛиз", "ЭБИС ", "ЮниМетр", "РКССочи", "Урожай", "ДрктЛиз",
       "ДЭНИКОЛБ", "Каскад", "ВейлФин", "ОВК Фин", "ДДёнер", "АрчерФин", "Пропфн", "ОВК Фин",
       "ЭНКО ", "МОСТРЕСТ ", "Труд ", "КПокров", "АПРИФП ", "ГрупПро", "ЧЗПСНП ", "ГарИнв", "ЗавдКЭС",
       "Шевченк", "Полипл", "Победа", "Держава", "ТАЛАНФБ", "ЛТрейд", "ИнДевел", "СОкс", 'ХКФин', 'РКС',
       "ОРГрупп", "РОСНАН", 'ЛИТАНА ', 'ТЕХЛиз ', "ТДСинтБО", "Калсб ", "ПЮДМ ", "ОхтаГр ", 'СЭЗ ', 'Аэрфью',
       'Оптима', "БЭЛТИ ", "Калита", "ЛЕГЕНДА", "НафттрнБО", "КарМаниБ", "КЛС БО", "МоторТ", "NexTouch", "ОхтаГр", "СНХТ ",
       "ГЛАВТОРГ", "Пропфин", "ТРИНФ Х ", "РегПрод", "СПМК "]

nonliqvid = []
cancel = []
no_cost = []
finished = []
no_date = []
last_deal = []
big_nom = []
kval = ["ВТБ Б1-", 'BCS', 'СберИОС', 'МКБ П0', 'ВТБ Б-1', 'ОткрФКБИО', 'МКБ П', "ТинькоффИ", "ОткрФКИОП", "СибЭнМаш",
        "ВЭББНКР ", "ОткрФКИО13", "ОткрБРСО7", "БКСБ1Р-01", "ЕАБР 1Р-04", "ГПБ002P-12", "МежИнБанк4", "ЛаймЗайм02", "ОткрБРСО9",
        "ОткрБРСО8", "МежИнБ01P5", "МежИнБ01P5", "МежИнБ01P1", "ОткрБРСО12", "ОткрБРСО11"]
kval1 = []
no_exist = ['МФК ЦФПО02', "МФК ЦФПО01", "МаниМен 03", "Займер 01", "АйДиКоле01", "МаниМен 02", "ОткрБРСО3",
            "ОткрБРСО5", 'ВЭББНКР 01', 'Займер 02', 'Маныч02', 'Брус 1P02', 'Займер 03', "Kviku1P1", "AAG-01", "ГПБ-КИ-02",
            "СФОВТБИП03", "ОткрБРСО6", "СФОВТБИП02", "АйДиКоле02", "ВЭББНКР 02", "АСПЭКДом01", "ЛаймЗайм01", "КарМани 01",
            "ОткрБРСО4"]


def extr(obl, text, a=False):
    """
    Извлечение значения по тексту
    """
    t = obl.findAll('abbr', string=re.compile(text))
    t = t[0]
    t = t.parent
    if a:
        t = t.parent
    return t.next_sibling.next_sibling.string


def nonl(obl):
    """Не берем бумаги, сделки по которым были более 3 дней назад"""
    target = obl.findAll("script")
    for s in target:
        if s.string is None:
            continue
        match = re.search(r'var aTradeResultsData = {(.+?)}', s.string)
        if match is None:
            continue
        match = '{' + match.group(1) + '}'
        d = json.loads(match)
        if not len(d['dates']):
            return True
        return (datetime.datetime.now() - datetime.datetime.strptime(d['dates'][-1], '%d.%m.%Y')).days > 3


def calc(site, day=True, cost=None):
    """
    Расчёт прибыли одной облигации
    """
    try:
        obl = BeautifulSoup(requests.get(site).text, 'lxml')
    except:
        return {}
    name = extr(obl, 'Название')
    if nonl(obl):
        nonliqvid.append(name)
        return {}
    if name in kval or name in no_exist:
        return {}
    try:
        nex = datetime.datetime.strptime(
            extr(obl, 'Дата след. выплаты'), '%d-%m-%Y')
    except ValueError:
        no_date.append(name)
        return {}
    except Exception:
        if day:
            # вытащить из таблицы купонов
            nex = datetime.datetime.strptime(obl.findAll(
                'table')[1].findAll('td')[1].string, '%d-%m-%Y')
    years = float(extr(obl, 'Лет до погашения'))
    days = int(years * 365)
    if years == 0:
        # не учитывать закончившиеся
        finished.append(name)
        return {}
    if cost is None:
        cost_p = extr(obl, 'Цена послед')
    else:
        # если цену задать самостоятельно
        cost_p = cost
    if cost_p:
        cost_p = float(cost_p)
    else:
        # не учитывать с неизвестной ценой
        no_cost.append(name)
        return {}
    nom = float(extr(obl, 'Номинал'))
    if nom > 1500:
        big_nom.append(name)
        return {}
    nkd = float(extr(obl, 'НКД', a=True).split()[0])
    # стоимость с учётом НКД и комиссией
    cost = (cost_p / 100 * nom + nkd) * 1.0006
    profit = 0  # прибыль
    profit_in = 0  # прибыль с учётом инфляции
    tab = obl.findAll('table')
    if len(tab) > 1:
        # если есть таблица с купонами
        tab = tab[1]
        for coup in tab.findAll('tr')[1:]:
            n = datetime.datetime.strptime(
                coup.contents[3].string, '%d-%m-%Y').date()
            delta = (n - datetime.datetime.today().date()).days
            if delta < 3:
                # если купон выплачивается послезавтра и раньше, не считается
                continue
            q = coup.contents[5].string
            if q == '—':
                # закончились известные купоны
                break
            inc = float(q) * 0.87  # купон за вычетом налогов
            profit += inc
            profit_in += inc * inf4day**delta
    perc = ((profit + nom) / cost - 1) * 100  # общая прибыль
    perc_g = perc / years  # прибыль в год
    if 'ОФЗ-ИН' in extr(obl, 'Имя облигации'):
        perc_i = ((profit_in + nom) / cost - 1) * \
            100  # офз с защитой от инфляции
    else:
        perc_i = ((profit_in + nom * inf4day**days) / cost - 1) * \
            100  # общая прибыль с учётом инфляции
    perc_in = perc_i / years  # прибыль в год с учётом инфляции
    if day:
        return {'Облигация': name,
                'Прибыль, %': round(perc, 3),
                'Годовая прибыль с учётом инфляции, %': round(perc_in, 3),
                'День': nex}
    else:
        return {'Облигация': name,
                'Прибыль, %': round(perc, 3),
                "Прибыль с учётом инфляции, %": round(perc_i, 3),
                'Годовая прибыль, %': round(perc_g, 3),
                "Годовая прибыль с учётом инфляции, %": round(perc_in, 3),
                "Дата погашения": datetime.datetime.now() + datetime.timedelta(days=days)}


def parse(table, site, day=True):
    print(site.split('/')[-2])
    try:
        lst = BeautifulSoup(requests.get(site).text, 'lxml')
    except:
        return table
    last_page = lst.findAll('a', {'class': 'page gradient last'})
    if last_page:
        last_page = last_page[1]['href']
        last_page = int(last_page[last_page.find('page') + len('page'):-1])
    else:
        last_page = 1

    print(f"Всего {last_page} страниц")
    for i in range(2, last_page + 2):
        # постраничный парсинг
        lst = lst.findAll('tr')
        lst = lst[3:] if day else lst[1:]
        for j, tr in enumerate(lst):
            flag = 0
            name = tr.contents[5].a.string if day else tr.contents[5].string
            for e in exc:
                # проверка на нахождение в чёрном списке
                if e in name:
                    flag = 1
                    cancel.append(name)
                    break
            for e in kval:
                # проверка на нахождение в списке для квалов
                if e in name:
                    flag = 1
                    kval1.append(name)
                    break
            if flag:
                continue
            st = calc('https://smart-lab.ru' + tr.contents[5].a["href"], day)
            if st:
                table = table.append(st, ignore_index=True)
        try:
            if day:
                lst = BeautifulSoup(requests.get(
                    site + f'/page{i}/').text, 'lxml')
            else:
                lst = BeautifulSoup(requests.get(
                    site + f'/order_by_val_to_day/desc/page{i}/').text, 'lxml')
        except:
            return table
        print(f"Страница {i - 1} закончена")
    return table


def calend():
    table = pd.DataFrame(columns=[
                         'Облигация', 'Прибыль, %', 'Годовая прибыль с учётом инфляции, %', 'День'])
    today = '01.10.2021'  # (datetime.datetime.today()).strftime('%d.%m.%Y')
    to = '15.10.2021'  # datetime.datetime.today() + datetime.timedelta(month=1)
    site = f'https://smart-lab.ru/calendar/bonds/country_0/from_{today}/to_{to}'
    table = parse(table, site)
    table = table.sort_values(
        by=['День', 'Годовая прибыль с учётом инфляции, %'], ascending=[True, False])
    # table = table.sort_values(by='Годовая прибыль, %', ascending=False)
    table.to_csv(path_or_buf='Итоги на каждый день.csv', sep=';')


def all_obl():
    sites = ["https://smart-lab.ru/q/ofz/",
             "https://smart-lab.ru/q/subfed/", "https://smart-lab.ru/q/bonds/"]
    table = pd.DataFrame(columns=['Облигация', 'Прибыль, %', "Прибыль с учётом инфляции, %", 'Годовая прибыль, %',
                                  "Годовая прибыль с учётом инфляции, %"])
    for site in sites:
        table = parse(table, site, day=False)
    table = table.sort_values(
        by=["Годовая прибыль с учётом инфляции, %"], ascending=[False])
    table.to_csv(path_or_buf='Итоги.csv', sep=',')


all_obl()
# print(calc('https://smart-lab.ru/q/bonds/RU000A1039F7/', day=False, cost='100'))
# calend()
print(f"Не рассмотрены ({len(cancel)}): {cancel}")
print(f"Сегодня закрытие ({len(finished)}): {finished}")
print(f"Не указана последняя цена ({len(no_cost)}): {no_cost}")
print(f"Не указана дата погашения ({len(no_date)}): {no_date}")
print(f"Дорогая стоимость ({len(big_nom)}): {big_nom}")
print(f"Только для квалифицированных инвестров ({len(kval1)}): {kval1}")
print(f"Неликвид ({len(nonliqvid)}): {nonliqvid}")
