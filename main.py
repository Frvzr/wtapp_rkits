import os
import pandas as pd
from math import floor
import xlsxwriter
import logging
import pathlib

DIR = os.getcwd()
FILE_PATH = f'{DIR}\\required_redress_kit.xlsx'

_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"

def get_file_handler():
    """_summary_

    Returns:
        _type_: _description_
    """
    file_handler = logging.FileHandler("log.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler

def get_stream_handler():
    """_summary_

    Returns:
        _type_: _description_
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler

def get_logger(name):
    """_summary_

    Args:
        name (_str_): _description_

    Returns:
        _type_: _description_
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler())
    logger.addHandler(get_stream_handler())
    return logger


logger = get_logger(__name__)


def get_data_from_excel(FILE_PATH):
    """
    Функция получает данные с файла

    Args:
        FILE_PATH (_str_): Путь к файлу с обрабатываемыми данными

    Returns:
        (_dict_): Функция возвращает данные с 3-х заранее известных страниц файла
    """
    try:
        logger.info('Start programm')
        redress = pd.read_excel(FILE_PATH, sheet_name='Required redress kits')
        required_redress_kits = {"series": []}
        for redress_kit, v in redress.groupby("Redress kit", sort=False):
            required_redress_kits["series"].append({"redress_kit": redress_kit.upper(), "total": []})
            for quantity_on_store, required_quantity in zip(v["Q-ty on store"], v["Req qty"]):
                required_redress_kits["series"][-1]["total"].append({"q-ty on store": quantity_on_store, "required": required_quantity})

        rk_bom = pd.read_excel(FILE_PATH, sheet_name='Redress kit BOM')
        redress_kit_bom = {"series": []}
        for redress_kit_, redress_kit_items in rk_bom.groupby("Redress Part Number"):
            redress_kit_bom["series"].append({"redress kit": redress_kit_.upper(), "consist": []})
            for w, s, t in zip(redress_kit_items["Item Part Number"], redress_kit_items["Quantity pr."], redress_kit_items['Description']):
                redress_kit_bom["series"][-1]["consist"].append({'item': w, 'description': t, 'qty': s})
        
        qty_on_store = pd.read_excel(FILE_PATH, sheet_name='Pivot Stock')
        qty_on_store_data = dict(zip(qty_on_store['Part Number'], qty_on_store['Sum of QTY']))

        return required_redress_kits, redress_kit_bom, qty_on_store_data
    except Exception as e:
        logger.critical(e)
        
        
def merge_consist(required_redress_kits, redress_kit_bom):
    """_summary_

    Args:
        need_redress_kits (_dict_): данные с необходимыми наборами зип
        redress_kit_bom (_dict_): данные с составляющими запчастями, входящие в набор зип

    Returns:
        (_dict_): данные с необходимым кол-вом наборов зип и их составом
    """
    try:
        required_with_items = {'series': []}
        for a, b in required_redress_kits.items():
            for z in b:
                for k, v in redress_kit_bom.items():
                    for i in v:
                        if i["redress kit"] == z['redress_kit']:
                            required_with_items['series'].append({'redress_kit':i['redress kit'], "total": z["total"], "consist": i["consist"]})
        return required_with_items
    except Exception as e:
        logger.critical(e)


def merge_store(qty_on_store_data, required_with_items):
    """_summary_

    Args:
        qty_on_store_data (dict): словарь с количеством зип на складе
        required_with_items (dict): словарь с необходимым кол-вом наборов и их составом

    Returns:
        dict: словарь с максимальным кол-вом наборов, которые мы можем собрать
    """
    try:
        max_collect_redress = {'series': []}
        for a, b in required_with_items.items():
            for i in b:
                qty_on_store = {'qty_on_store': []}
                max_collect_items = {'max_collect_items': []}
                for y in i['consist']:
                    for k, v in qty_on_store_data.items():
                        if y['item'] == k.upper():
                            max_collect_item = floor(int(v) / int(y['qty']))
                            max_collect_items['max_collect_items'].append({"item": k.upper(), "qty": max_collect_item})
                            qty_on_store['qty_on_store'].append({'item': k, 'qty': v})

                required = i['total'][-1]['required']        
                res = get_min_data(max_collect_items)
                if not pd.isna(required) and res > required:
                    res = required
                if res > 0:    
                    qty_on_store_data = update_store(qty_on_store_data, i['consist'], res)
                
                max_collect_redress['series'].append({'redress_kit':i['redress_kit'], "total": i["total"], "consist": i["consist"], "max_collect_items": max_collect_items["max_collect_items"], "maximum_collect": res, 'qty_on_store': qty_on_store['qty_on_store']})
        return max_collect_redress
    except Exception as e:
        logger.critical(e)
                        

def get_min_data(data):
    try:
        min_data = []
        if data['max_collect_items']:
            for k, v in data.items():
                for i in v:
                    min_data.append(i['qty'])
            return min(min_data)
        else:
            return 0
    except  Exception as e:
        logger.error(f'{data} - {e}')


def update_store(qty_on_store_data, required_items, res):
    try:
        for i in required_items:
            for k, v in qty_on_store_data.items():  
                if i['item'] == k.upper():
                    qty_on_store_data[k] = v - (int(i['qty']) * res)
        return qty_on_store_data
    except Exception as e:
        logger.error(e)



# def handling_data(data):
#     try:
#         out_data = {'Redress Kit':[],
#                     'Qty on store': [],
#                     'Required': [],
#                     'Can collect': [],
#                     'Comment':[]}
        
#         for i in data["series"]:
            
#             required = i['total'][-1]['required']
#             minimum_redress = i['minimum_redress']
            
#             out_data["Redress Kit"].append(i['redress_kit'])
#             out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
#             out_data['Required'].append(required)
#             out_data['Can collect'].append(minimum_redress)
            
#             if pd.isna(required) and minimum_redress == 0:
#                 required = 1
#             if minimum_redress < required or minimum_redress == 0:
#                 comment = f'Не хватает до {required}: '
#                 for j in i['consist']:
#                     need_qty = j['qty'] * required
#                     for x in i['qty_on_store']:
#                         if j['item'].upper() == x['item'].upper() and need_qty > x['qty']:
#                             comment += f"{x['item']} - {need_qty - x['qty']} шт, "
#                 #print(comment)
#                 out_data['Comment'].append(comment)
#             else:
#                 out_data['Comment'].append('N/a')
#             #print(i['redress_kit'], out_data['Comment'])
#         #print(out_data)
#         return out_data
#     except Exception as e:
#         logger.error(e)

def handling_data(data):
    out_data = {'Redress Kit':[],
                'Qty on store': [],
                'Required': [],
                'Can collect': [],
                'Item': [],
                'Description': [],
                'Need to order': []}
    try:
        for i in data['series']:
            required = i['total'][-1]['required']
            max_collect = i['maximum_collect']
            
            if pd.isna(required) and max_collect == 0:
                 required = 1
            elif pd.isna(required) and max_collect > 0:
                required = max_collect
            for j in i['consist']:
                need_qty = j['qty'] * required
                for a in i['qty_on_store']:
                    if j['item'].upper() == a['item'].upper() and need_qty > a['qty']:
                        out_data["Redress Kit"].append(i['redress_kit'])
                        out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
                        out_data['Required'].append(required)
                        out_data['Can collect'].append(max_collect)
                        out_data['Item'].append(a['item'])
                        out_data['Description'].append(j['description'])
                        out_data['Need to order'].append(need_qty - a['qty']) 
                    elif required <= max_collect and i['redress_kit'] not in out_data["Redress Kit"]:
                        out_data["Redress Kit"].append(i['redress_kit'])                        #
                        out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])        # NOT DRY - надо переделать
                        out_data['Required'].append(required)                                   #
                        out_data['Can collect'].append(max_collect)                             #
                        out_data['Item'].append('N/a')
                        out_data['Description'].append('N/a')
                        out_data['Need to order'].append('N/a')
        return out_data
    
    except Exception as e:
        logger.error(e)
        
        
def output_data(all_data):
    try:
        out_path = pathlib.Path('can_collect_redress_kits.xlsx')
        with pd.ExcelWriter('can_collect_redress_kits.xlsx', engine="openpyxl", mode='a', if_sheet_exists="overlay") if out_path.exists() else pd.ExcelWriter('can_collect_redress_kits.xlsx', engine="openpyxl", mode='w') as wb:
            df = pd.DataFrame(all_data)
            df.to_excel(wb, sheet_name='Sheet1', index=False)
            ws = wb.sheets['Sheet1']
            ws.column_dimensions['A'].width = 15
            ws.column_dimensions['B'].width = 12
            
            # форматирование для xlsxwriter
            # sheet.freeze_panes('A2')
            # sheet.autofilter(0, 0, 0, 6)
            # sheet.set_default_row(20)
            # sheet.set_column(0, 0, 15)
            # sheet.set_column('B:D', 12)
            # sheet.set_column('E:E', 15)
            # sheet.set_column('F:F', 30)
            # sheet.set_column('G:G', 12)
            logger.info('End programm')
    except Exception as e:
        logger.error(e)
            
            
def main():
    required_redress_kits, redress_kit_bom, qty_on_store_data = get_data_from_excel(FILE_PATH)
    required_with_items = merge_consist(required_redress_kits, redress_kit_bom)
    raw_data = merge_store(qty_on_store_data, required_with_items)
    all_data = handling_data(raw_data)
    output_data(all_data)
    
    
if __name__ == '__main__':
    main()