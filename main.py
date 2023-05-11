import os
import pandas as pd
from math import floor
import xlsxwriter
import logging


DIR = os.getcwd()
FILE_PATH = f'{DIR}\\test_file.xlsx' 
_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"

def get_file_handler():
    file_handler = logging.FileHandler("test.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler

def get_stream_handler():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler

def get_logger(name):
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
        FILE_PATH (str): Путь к файлу с обрабатываемыми данными

    Returns:
        dict: Функция возвращает данные с 3-х, заранее известных страниц файла
    """
    try:
        logger.info('Start programm')
        redress = pd.read_excel(FILE_PATH, sheet_name='Redress')
        need_redress_kits = {"series": []}
        for k, v in redress.groupby("Redress kit", sort=False):
            need_redress_kits["series"].append({"redress_kit": k.upper(), "total": []})
            for q, r in zip(v["Q-ty on store"], v["Req qty"]):
                need_redress_kits["series"][-1]["total"].append({"q-ty on store": q, "required": r})

        rk_bom = pd.read_excel(FILE_PATH, sheet_name='redress_kits_items')
        redress_kit_bom = {"series": []}
        for i, g in rk_bom.groupby("Redress Part Number"):
            redress_kit_bom["series"].append({"redress kit": i.upper(), "consist": []})
            for w, s, t in zip(g["Item Part Number"], g["Quantity pr."], g['Description']):
                redress_kit_bom["series"][-1]["consist"].append({'item': w, 'description': t, 'qty': s})
        
        qty_on_store = pd.read_excel(FILE_PATH, sheet_name='Pivot Stock')
        qty_on_store_data = dict(zip(qty_on_store['Row Labels'], qty_on_store['Sum of QTY']))

        return need_redress_kits, redress_kit_bom, qty_on_store_data
    except Exception as e:
        logger.critical(e)
        
        
def merge_consist(need_redress_kits, redress_kit_bom):
    """_summary_

    Args:
        need_redress_kits (dict): _description_
        redress_kit_bom (dict): _description_

    Returns:
        dict: _description_
    """
    try:
        required_with_items = {'series': []}
        for a, b in need_redress_kits.items():
            for z in b:
                for k, v in redress_kit_bom.items():
                    for i in v:
                        if i["redress kit"] == z['redress_kit']:
                            required_with_items['series'].append({'redress_kit':i['redress kit'], "total": z["total"], "consist": i["consist"]})
        #print(required_with_items)
        return required_with_items
    except Exception as e:
        logger.critical(e)


def merge_store(qty_on_store_data, required_with_items):
    try:
        nd = {'series': []}
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
                #print(i['redress_kit'], max_collect_items)
                required = i['total'][-1]['required']        
                res = get_min_data(max_collect_items)
                if not pd.isna(required) and res > required:
                    res = required
                if res > 0:    
                    qty_on_store_data = update_store(qty_on_store_data, i['consist'], res)
                
                nd['series'].append({'redress_kit':i['redress_kit'], "total": i["total"], "consist": i["consist"], "max_collect_items": max_collect_items["max_collect_items"], "minimum_redress": res, 'qty_on_store': qty_on_store['qty_on_store']})
        #print(nd)
        return nd
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
                'Need to order Qty': []}
    
    for i in data['series']:
        required = i['total'][-1]['required']
        minimum_redress = i['minimum_redress']
        
        for j in i['consist']:
            need_qty = j['qty'] * required
            for a in i['qty_on_store']:
                if j['item'].upper() == a['item'].upper() and need_qty > a['qty']:
                    out_data["Redress Kit"].append(i['redress_kit'])
                    out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
                    out_data['Required'].append(required)
                    out_data['Can collect'].append(minimum_redress)
                    out_data['Item'].append(a['item'])
                    out_data['Description'].append(j['description'])
                    out_data['Need to order Qty'].append(need_qty - a['qty']) 
                elif required <= minimum_redress and i['redress_kit'] not in out_data["Redress Kit"]:
                    out_data["Redress Kit"].append(i['redress_kit'])                        #
                    out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])        # NOT DRY - надо переделать
                    out_data['Required'].append(required)                                   #
                    out_data['Can collect'].append(minimum_redress)                         #
                    out_data['Item'].append('N/a')
                    out_data['Description'].append('N/a')
                    out_data['Need to order Qty'].append('N/a')
                 
    return(out_data)

def output_data(all_data):
    try:
        logger.info('End programm')
        writer_obj = pd.ExcelWriter('test.xlsx', engine='xlsxwriter')
        df = pd.DataFrame(all_data)
        df.to_excel(writer_obj, sheet_name='Sheet', index=False)
        writer_obj._save()
    except Exception as e:
        logger.error(e)
            
            
def main():
    need_redress_kits, redress_kit_bom, qty_on_store_data = get_data_from_excel(FILE_PATH)
    required_with_items = merge_consist(need_redress_kits, redress_kit_bom)
    raw_data = merge_store(qty_on_store_data, required_with_items)
    all_data = handling_data(raw_data)
    output_data(all_data)
    
    
if __name__ == '__main__':
    main()