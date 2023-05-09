import os
import pandas as pd
from math import floor
import xlsxwriter


DIR = os.getcwd()
FILE_PATH = f'{DIR}\\test_file.xlsx' 


def get_data_from_excel(FILE_PATH):
    """
    Функция получает данные с файла

    Args:
        FILE_PATH (str): Путь к файлу с обрабатываемыми данными

    Returns:
        dict: Функция возвращает данные с 3-х, заранее известных страниц файла
    """
    
    redress = pd.read_excel(FILE_PATH, sheet_name='Redress')
    need_redress_kits = {"series": []}
    for k, v in redress.groupby("Redress kit", sort=False):
        need_redress_kits["series"].append({"redress_kit": k, "total": []})
        for q, r in zip(v["Q-ty on store"], v["Req qty"]):
            need_redress_kits["series"][-1]["total"].append({"q-ty on store": q, "required": r})

    rk_bom = pd.read_excel(FILE_PATH, sheet_name='redress_kits_items')
    redress_kit_bom = {"series": []}
    for i, g in rk_bom.groupby("Redress Part Number"):
        redress_kit_bom["series"].append({"redress kit": i, "consist": []})
        for w, s in zip(g["Item Part Number"], g["Quantity pr."]):
            redress_kit_bom["series"][-1]["consist"].append({'item': w, 'qty': s})

    qty_on_store = pd.read_excel(FILE_PATH, sheet_name='Pivot Stock')
    qty_on_store_data = dict(zip(qty_on_store['Row Labels'], qty_on_store['Sum of QTY']))

    return need_redress_kits, redress_kit_bom, qty_on_store_data


def merge_consist(need_redress_kits, redress_kit_bom):
    """_summary_

    Args:
        need_redress_kits (dict): _description_
        redress_kit_bom (dict): _description_

    Returns:
        dict: _description_
    """
    
    required_with_items = {'series': []}
    for a, b in need_redress_kits.items():
        for z in b:
            for k, v in redress_kit_bom.items():
                for i in v:
                    if i["redress kit"] == z['redress_kit']:
                        required_with_items['series'].append({'redress_kit':i['redress kit'], "total": z["total"], "consist": i["consist"]})
    return required_with_items


def merge_store(qty_on_store_data, required_with_items):
    nd = {'series': []}
    for a, b in required_with_items.items():
        for i in b:
            qty_on_store = {'qty_on_store': []}
            max_collect_items = {'max_collect_items': []}
            for y in i['consist']:
                for k, v in qty_on_store_data.items():
                    if y['item'] == k:
                        max_collect_item = floor(int(v) / int(y['qty']))
                        max_collect_items['max_collect_items'].append({"item": k, "qty": max_collect_item})
                        qty_on_store['qty_on_store'].append({'item': k, 'qty': v})
            required = i['total'][-1]['required']        
            res = get_min_data(max_collect_items)
            if not pd.isna(required) and res > required:
                res = required
            if res > 0:    
                qty_on_store_data = update_store(qty_on_store_data, i['consist'], res)
            
            nd['series'].append({'redress_kit':i['redress_kit'], "total": i["total"], "consist": i["consist"], "max_collect_items": max_collect_items["max_collect_items"], "minimum_redress": res, 'qty_on_store': qty_on_store['qty_on_store']})
    return nd
                        

def get_min_data(data):
    min_data = []
    for k, v in data.items():
        for i in v:
            min_data.append(i['qty'])
    return min(min_data)



def update_store(qty_on_store_data, required_items, res):
    for i in required_items:
        for k, v in qty_on_store_data.items():  
            if i['item'] == k:
                qty_on_store_data[k] = v - (int(i['qty']) * res)
    return qty_on_store_data
    

def handling_data(data):
    out_data = {'Redress Kit':[],
                'Qty on store': [],
                'Required': [],
                'Can collect': [],
                'Comment':[]}
    
    for i in data["series"]:
        
        required = i['total'][-1]['required']
        minimum_redress = i['minimum_redress']
        
        out_data["Redress Kit"].append(i['redress_kit'])
        out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
        out_data['Required'].append(required)
        out_data['Can collect'].append(minimum_redress)
        
        if pd.isna(required) and minimum_redress == 0:
            required = 1
        if minimum_redress < required or minimum_redress == 0:
            comment = ''
            for j in i['consist']:
                need_qty = j['qty'] * required
            out_data['Comment'].append('Yes')
        else:
            out_data['Comment'].append('N/a')
        print(i['redress_kit'], out_data['Comment'])
    print(out_data)
    return out_data


def output_data(all_data):
    writer_obj = pd.ExcelWriter('test.xlsx', engine='xlsxwriter')
    df = pd.DataFrame(all_data)
    df.to_excel(writer_obj, sheet_name='Sheet')
    writer_obj._save()
    

def main():
    need_redress_kits, redress_kit_bom, qty_on_store_data = get_data_from_excel(FILE_PATH)
    required_with_items = merge_consist(need_redress_kits, redress_kit_bom)
    raw_data = merge_store(qty_on_store_data, required_with_items)
    all_data = handling_data(raw_data)
    output_data(all_data)
    
    
    
if __name__ == '__main__':
    main()