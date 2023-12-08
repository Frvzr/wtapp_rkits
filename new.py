import os
import pandas as pd
from math import floor
import pathlib
from openpyxl.styles import Alignment
from logger import get_logger

logger = get_logger(__name__)

DIR = os.getcwd()
INPUT_FILE = 'required_redress_kit.xlsx'
FILE_PATH = f'{DIR}\\{INPUT_FILE}'
SHEETNAME_INPUT_STOCK = 'Stock 2023'
SHEETNAME_INPUT_REQUIRED = 'Required redress kits'
SHEETNAME_INPUT_BOM = 'Redress kit BOM'

OUTPUT_FILE = "can_collect_redress_kits.xlsx"
SHEETNAME_OUTPUT = 'Collect Redress Kit'
SHEETNAME_STORE_OUTPUT = 'Updated stocks'


def get_data_from_excel_stock(path):
    try:
        logger.info('Start programm')
        stock_dataframe = pd.read_excel(path, sheet_name=SHEETNAME_INPUT_STOCK)
        
        agg_qty_sum = {'QTY':['sum']}
        data_by_stock = stock_dataframe.groupby(['Part Number', 'Stock']).agg(agg_qty_sum).to_dict()
        
        items_by_stock = {'Main': {}, 'Ru Ops': {}}
        for data in data_by_stock.values():
            for data_from_stock, qty in data.items():
                if data_from_stock[1].lower() == 'main':
                    items_by_stock['Main'][data_from_stock[0]] = qty
                else:
                    items_by_stock['Ru Ops'][data_from_stock[0]] = qty

        total = {k: items_by_stock['Main'].get(k, 0) + items_by_stock['Ru Ops'].get(k, 0) for k in set(items_by_stock['Main']) | set(items_by_stock['Ru Ops'])}

        serial_items = stock_dataframe.groupby(['Part Number', 'Stock', 'SN']).agg(agg_qty_sum).to_dict()
        actual_serial_items = {}
        for items_sn in serial_items.values():
            for item_sn, qty_sn in items_sn.items():
                if qty_sn == 0:
                    continue
                else:
                    actual_serial_items[item_sn] = qty_sn
        print(f"Data from {SHEETNAME_INPUT_STOCK} uploaded")
        return items_by_stock, total, actual_serial_items
    except ValueError as ve:
        logger.critical(f"{ve} in the {INPUT_FILE}")
    except KeyError as ke:
        logger.critical(f"Please change the title of column back to recommended {ke}")
    except Exception as e:
        logger.critical(e)



def get_data_from_excel_required_redress(path):
    try:
        redress = pd.read_excel(path, sheet_name=SHEETNAME_INPUT_REQUIRED)
        required_redress_kits = {"Required redress kit": []}
        for redress_kit, req_qty in redress.groupby("Redress kit", sort=False):
            required_redress_kits["Required redress kit"].append({"redress_kit": redress_kit.upper(), "total": []})
            for quantity_on_store, required_quantity in zip(req_qty["Q-ty on store"], req_qty["Req qty"]):
                required_redress_kits["Required redress kit"][-1]["total"].append({"q-ty on store": quantity_on_store, "required": required_quantity})
        print(f"Data from {SHEETNAME_INPUT_REQUIRED} uploaded")
        return required_redress_kits
    except ValueError as ve:
        logger.critical(f"{ve} in the {INPUT_FILE}")
    except KeyError as ke:
        logger.critical(f"Please change the title of column back to recommended {ke}")
    except Exception as e:
        logger.critical(e)
    
def get_data_from_excel_redress_kits_bom(path):
    try:
        rk_bom = pd.read_excel(path, sheet_name=SHEETNAME_INPUT_BOM)
        redress_kit_bom = {"redress kit consist": []}
        for redress_kit_, redress_kit_items in rk_bom.groupby("Redress Part Number"):
            redress_kit_bom["redress kit consist"].append({"redress kit": redress_kit_.upper(), "consist": []})
            for item, qty, desc in zip(redress_kit_items["Item Part Number"], redress_kit_items["Quantity pr."], redress_kit_items['Description']):
                if qty == 0 or qty == '0':
                    continue
                else:
                    redress_kit_bom["redress kit consist"][-1]["consist"].append({'item': item, 'description': desc, 'qty': qty})
        print(f"Data from {SHEETNAME_INPUT_BOM} uploaded")
        return redress_kit_bom
    except ValueError as ve:
        logger.critical(f"{ve} in the {INPUT_FILE}")
    except KeyError as ke:
        logger.critical(f"Please change the title of column back to recommended {ke}")
    except Exception as e:
        logger.critical(e)

def merge_consist(required_redress_kits, redress_kit_bom):
    required_with_items = {'Items for redres kits': []}
    for req_kit in required_redress_kits['Required redress kit']:
        for consist in redress_kit_bom["redress kit consist"]:
            if consist["redress kit"] == req_kit['redress_kit']:
                required_with_items['Items for redres kits'].append({'redress_kit': consist['redress kit'], "total": req_kit["total"], "consist": consist["consist"]})
    print("Merging required redress kits and BOM has been completed")
    return required_with_items


def merge_store(qty_on_store_data, required_with_items, serial_items):
    max_collect_redress = {'maximum collect rkits': []}
    for item in required_with_items['Items for redres kits']:
        required = item['total'][-1]['required']
        qty_on_store = {'qty_on_store': []}
        max_collect_items = {'max_collect_items': []}
        reserved = {'reserved': []}
        serial = {'serial_items': []}
        for y in item['consist']:
            if y['item'] not in qty_on_store_data:
                print(f"This item: {y['item']} is out of stock")
                qty_on_store_data[y['item']] = 0
            for k, v in qty_on_store_data.items():
                if y['item'] == k.upper():
                    max_collect_item = floor(v / y['qty'])
                    max_collect_items['max_collect_items'].append({"item": k.upper(), "qty": max_collect_item})
                    qty_on_store['qty_on_store'].append({'item': k, 'qty': v})
                    if not pd.isna(required):
                        reserved = get_reserved(required, item, qty_on_store_data)
            
            for sn_item, sn_qty in serial_items.items():
                if sn_item[0] == y['item']:
                    serial['serial_items'].append(({'sn_item': sn_item[0], 'store': sn_item[1], 'serial_number': sn_item[2], 'sn_qty': sn_qty}))
        
                           
        res = get_min_data(max_collect_items)
        if not pd.isna(required) and res > required:
            res = required
        if pd.isna(required):
            reserved = get_reserved(res, item, qty_on_store_data)
        qty_on_store_data = update_store(qty_on_store_data, reserved['reserved'])    
        max_collect_redress['maximum collect rkits'].append({'redress_kit':item['redress_kit'], "total": item["total"], "consist": item["consist"], "max_collect_items": max_collect_items["max_collect_items"], "maximum_collect": res, 'qty_on_store': qty_on_store['qty_on_store'], 'reserved': reserved['reserved'], 'serial': serial['serial_items']})
    print("Merging data with stock has been completed")
    return max_collect_redress, qty_on_store_data


def get_reserved(res, redress, qty_on_store_data):
    reserved = {'reserved': []}
    required = res if res >= 0 else 1
    for item in redress['consist']:
        item_from_consist = item['item']
        qty_from_consist = item['qty']
        for key, value in qty_on_store_data.items():
            qty_from_store = value
            item_from_store = key.upper()
            if item_from_consist == item_from_store:
                if qty_from_store > 0:
                    req = qty_from_consist * int(required)
                    reserv = qty_from_store - req 
                    if reserv >= 0:
                        reserved['reserved'].append({"item": item_from_store, "qty": req})
                    elif reserv < 0:
                        reserved['reserved'].append({"item": item_from_store, "qty": qty_from_store})
                else:
                    reserved['reserved'].append({"item": item_from_store, "qty": 0})
    return reserved


def get_min_data(data):
    min_data = []
    if data['max_collect_items']:
        for k, v in data.items():
            for i in v:
                min_data.append(i['qty'])
        return min(min_data)
    else:
        return 0


def update_store(qty_on_store_data, reserved):      
    for j in reserved:
        for k, v in qty_on_store_data.items():  
            if j['item'] == k.upper() and (v - j['qty']) > 0:
                qty_on_store_data[k] = v - j['qty']                             
            elif j['item'] == k.upper() and (v - j['qty']) <= 0:
                qty_on_store_data[k] = 0
    return qty_on_store_data


def data_handling_items(data, items_by_stock):
    out_data = {'Redress Kit':[],
                'Qty on store': [],
                'Required': [],
                'Can collect': [],
                'Item': [],
                'Qty per kit':[],
                'Description': [],
                'Need to order': [],
                'Reserved':[],
                'Serial Number': [],
                'Main': [],
                'Ru Ops': [],
    }
    
    for i in data['maximum collect rkits']:
        required = i['total'][-1]['required']
        max_collect = i['maximum_collect']
        if pd.isna(required) and max_collect == 0:
                 required = 1
        elif pd.isna(required) and max_collect > 0:
            required = max_collect
        for j in i['consist']:
            req_item = j['item'].upper()
            need_qty = j['qty'] * required
            item_from_main = items_by_stock['Main'].get(req_item, 0)
            item_from_ruops = items_by_stock['Ru Ops'].get(req_item, 0)
            for a in i['qty_on_store']:
                for b in i['reserved']:
                    if req_item  == a['item'].upper() and req_item  == b['item'].upper():
                        for q in i['serial']:
                            sn_item = q['sn_item']
                            sn_store = q['store']
                            sn = q['serial_number']
                            sn_qty = q['sn_qty']
                            if req_item == sn_item:
                                out_data['Serial Number'].append(sn)
                                if sn_store == 'Main':
                                    out_data['Main'].append(sn_qty)
                                    out_data['Ru Ops'].append(0) 
                                elif sn_store == 'RU Ops':
                                    out_data['Ru Ops'].append(sn_qty)
                                    out_data['Main'].append(0)
                                    
                                out_data["Redress Kit"].append(i['redress_kit'])
                                out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
                                out_data['Required'].append(required)
                                out_data['Can collect'].append(max_collect)
                                out_data['Item'].append(a['item'])
                                out_data['Qty per kit'].append(j['qty'])
                                out_data['Description'].append(j['description'])
                                if (need_qty - a['qty']) > 0:
                                    out_data['Need to order'].append(need_qty - a['qty'])
                                else:
                                    out_data['Need to order'].append(0)
                                out_data['Reserved'].append(b['qty'])
                                
                        else:
                                out_data['Serial Number'].append(0)
                                out_data["Redress Kit"].append(i['redress_kit'])
                                out_data['Qty on store'].append(i['total'][-1]['q-ty on store'])
                                out_data['Required'].append(required)
                                out_data['Can collect'].append(max_collect)
                                out_data['Item'].append(a['item'])
                                out_data['Qty per kit'].append(j['qty'])
                                out_data['Description'].append(j['description'])
                                if (need_qty - a['qty']) > 0:
                                    out_data['Need to order'].append(need_qty - a['qty'])
                                else:
                                    out_data['Need to order'].append(0)
                                out_data['Reserved'].append(b['qty'])
                                out_data['Main'].append(item_from_main)
                                out_data['Ru Ops'].append(item_from_ruops)
    print("The data has been collected")
    return out_data


def data_handling_store(updated_store):
    store_data = {'Item': [], 'Quantity': []}
    for item_pn, qty in updated_store.items():
        store_data['Item'].append(item_pn)
        store_data['Quantity'].append(qty) 
    print("The stock has been updated")             
    return store_data


def bg_header(x):
    return "background-color: #9ccc65"


def get_text_color(val):
    color = 'red' if val == 'Need to order' else 'black'
    return 'color: %s' % color


def get_center_text(val):
    return 'text-align: center'

    
def format_sheets(col_dims, ws):
    for col_name, col_width in col_dims.items():
        col_name_store_num = col_name + '1'
        ws.column_dimensions[col_name].width = col_width
        ws[col_name_store_num].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        
def output_data(all_data, store_data):
    
    out_path = pathlib.Path(OUTPUT_FILE)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode='a', if_sheet_exists="replace") if out_path.exists() else pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode='w') as wb:
        df = pd.DataFrame(all_data)
        df_store = pd.DataFrame(store_data)
        
        df.style.applymap_index(bg_header, axis=1).applymap_index(get_text_color, axis=1).applymap_index(get_center_text, axis=1).set_properties(**{'text-align': 'center'}).to_excel(wb, sheet_name=SHEETNAME_OUTPUT, index=False)
        df_store.to_excel(wb, sheet_name=SHEETNAME_STORE_OUTPUT, index=False)
        df_store.style.applymap_index(bg_header, axis=1).applymap_index(get_text_color, axis=1).applymap_index(get_center_text, axis=1).set_properties(**{'text-align': 'center'}).to_excel(wb, sheet_name=SHEETNAME_STORE_OUTPUT, index=False)
        
        ws = wb.sheets[SHEETNAME_OUTPUT]
        ws.auto_filter.ref='a:l' 
        
        col_dims_items = {'A': 13, 'B': 8, 'C': 10, 'D': 10, 'E': 12, 'F': 8, 'G': 30, 'H': 10, 'I': 10, 'J': 10, 'K': 10, 'L': 10}
        col_dims_store = {'A': 25, 'B': 11}
        
        format_sheets(col_dims_items, ws)
        
        ws_store = wb.sheets[SHEETNAME_STORE_OUTPUT]
        ws_store.auto_filter.ref='a:b'
        
        format_sheets(col_dims_store, ws_store)
    print(f"The file {OUTPUT_FILE} has been created or updated")
    logger.info('Data collection and calculation completed')           
            
def main():
    try:
        items_by_stock, total, serial_items = get_data_from_excel_stock(FILE_PATH)
        required_redress_kits = get_data_from_excel_required_redress(FILE_PATH)
        redress_kit_bom = get_data_from_excel_redress_kits_bom(FILE_PATH)
        required_with_items = merge_consist(required_redress_kits, redress_kit_bom)
        data, updated_store = merge_store(total, required_with_items, serial_items)
        all_data = data_handling_items(data, items_by_stock)
        store_data = data_handling_store(updated_store)
        output_data(all_data, store_data)
    except PermissionError:
        print(f"Please close file: {OUTPUT_FILE}")
    except FileNotFoundError as fnf:
        logger.critical(f"File not found: {INPUT_FILE}")
    except Exception as e:
        logger.critical(e)

if __name__ == '__main__':
    main()