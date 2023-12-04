import os
import pandas as pd
from math import floor
import logging
import pathlib
from openpyxl.styles import Alignment

DIR = os.getcwd()
FILE_PATH = f'{DIR}\\required_redress_kit.xlsx'


def get_data_from_excel_stock(path):
    stock_dataframe = pd.read_excel(path, sheet_name='Stock 2023')
    
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

    return items_by_stock, total, actual_serial_items


def get_data_from_excel_required_redress(path):
    redress = pd.read_excel(path, sheet_name='Required redress kits')
    required_redress_kits = {"Required redress kit": []}
    for redress_kit, req_qty in redress.groupby("Redress kit", sort=False):
        required_redress_kits["Required redress kit"].append({"redress_kit": redress_kit.upper(), "total": []})
        for quantity_on_store, required_quantity in zip(req_qty["Q-ty on store"], req_qty["Req qty"]):
            required_redress_kits["Required redress kit"][-1]["total"].append({"q-ty on store": quantity_on_store, "required": required_quantity})
    return required_redress_kits
    
    
def get_data_from_excel_redress_kits_bom(path):
    rk_bom = pd.read_excel(path, sheet_name='Redress kit BOM')
    redress_kit_bom = {"redress kit consist": []}
    for redress_kit_, redress_kit_items in rk_bom.groupby("Redress Part Number"):
        redress_kit_bom["redress kit consist"].append({"redress kit": redress_kit_.upper(), "consist": []})
        for item, qty, desc in zip(redress_kit_items["Item Part Number"], redress_kit_items["Quantity pr."], redress_kit_items['Description']):
            if qty == 0 or qty == '0':
                #print(redress_kit_, item, qty, desc)
                continue
            else:
                redress_kit_bom["redress kit consist"][-1]["consist"].append({'item': item, 'description': desc, 'qty': qty})
    return redress_kit_bom


def merge_consist(required_redress_kits, redress_kit_bom):
    required_with_items = {'Items for redres kits': []}
    for req_kit in required_redress_kits['Required redress kit']:
        for consist in redress_kit_bom["redress kit consist"]:
            if consist["redress kit"] == req_kit['redress_kit']:
                required_with_items['Items for redres kits'].append({'redress_kit': consist['redress kit'], "total": req_kit["total"], "consist": consist["consist"]})
    return required_with_items


def merge_store(qty_on_store_data, required_with_items, serial_items):
    max_collect_redress = {'maximum collect rkits': []}
    for item in required_with_items['Items for redres kits']:
        required = item['total'][-1]['required']
        qty_on_store = {'qty_on_store': []}
        max_collect_items = {'max_collect_items': []}
        reserved = {'reserved': []}
        serial_items = {}
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
        res = get_min_data(max_collect_items)
        if not pd.isna(required) and res > required:
            res = required
        if pd.isna(required):
            reserved = get_reserved(res, item, qty_on_store_data)
        qty_on_store_data = update_store(qty_on_store_data, reserved['reserved'])
                
        max_collect_redress['maximum collect rkits'].append({'redress_kit':item['redress_kit'], "total": item["total"], "consist": item["consist"], "max_collect_items": max_collect_items["max_collect_items"], "maximum_collect": res, 'qty_on_store': qty_on_store['qty_on_store'], 'reserved': reserved['reserved']})
    return max_collect_redress, qty_on_store_data


def get_reserved(res, redress, qty_on_store_data):
    reserved = {'reserved': []}
    # balance = {'balance': []}
    # main = {'main': []}
    # ru_ops = {'ruops': []}
    required = res if res >= 0 else 1
    for y in redress['consist']:
        for k, v in qty_on_store_data.items():
            if y['item'] == k.upper():
                # total_stock = items_by_stock['Main'].get(y['item'], 0) + items_by_stock['Ru Ops'].get(y['item'], 0)
                if v > 0:
                    req = y['qty'] * int(required)
                    reserv = v - req
                    
                    # if items_by_stock['Main'][y['item']] - (total_stock - reserv) >= 0:
                    #     main['main'].append({"item": k.upper(), "qty": items_by_stock['Main'][y['item']] - (total_stock - reserv)})
                    #     ru_ops['ruops'].append({"item": k.upper(), "qty": items_by_stock['Ru Ops'].get(y['item'], 0)})
                    # elif items_by_stock['Main'][y['item']] - (total_stock - reserv) < 0:
                    #     main['main'].append({"item": k.upper(), "qty": 0})
                    #     if y['item'] in items_by_stock['Ru Ops'] and items_by_stock['Ru Ops'][y['item']] - (total_stock - reserv) >= 0:
                    #         ru_ops['ruops'].append({"item": k.upper(), "qty": items_by_stock['Ru Ops'][y['item']] - ((total_stock - reserv) - items_by_stock['Main'][y['item']])})
                    #     else:
                    #         ru_ops['ruops'].append({"item": k.upper(), "qty": 0})
                            
                    if reserv >= 0:
                        reserved['reserved'].append({"item": k.upper(), "qty": req})
                        # balance['balance'].append({"item": k.upper(), "qty": v - req})
                    elif reserv < 0:
                        reserved['reserved'].append({"item": k.upper(), "qty": v})
                        # balance['balance'].append({"item": k.upper(), "qty": 0})
                else:
                    reserved['reserved'].append({"item": k.upper(), "qty": 0})
                    # balance['balance'].append({"item": k.upper(), "qty": 0})
                    # ru_ops['ruops'].append({"item": k.upper(), "qty": 0})
                    # main['main'].append({"item": k.upper(), "qty": 0})
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


def handling_data(data, updated_store, items_by_stock):
    out_data = {'Redress Kit':[],
                'Qty on store': [],
                'Required': [],
                'Can collect': [],
                'Item': [],
                'Qty per kit':[],
                'Description': [],
                'Need to order': [],
                'Reserved':[],
                'Main': [],
                'Ru Ops': []
    }
    
    store_data = {'Item': [],
                  'Quantity': []}
    
    for i in data['maximum collect rkits']:
        required = i['total'][-1]['required']
        max_collect = i['maximum_collect']
        if pd.isna(required) and max_collect == 0:
                 required = 1
        elif pd.isna(required) and max_collect > 0:
            required = max_collect
        for j in i['consist']:
            need_qty = j['qty'] * required
            item_from_main = items_by_stock['Main'].get(j['item'], 0)
            item_from_ruops = items_by_stock['Ru Ops'].get(j['item'], 0)
            for a in i['qty_on_store']:
                for b in i['reserved']:
                    if j['item'].upper() == a['item'].upper() and j['item'].upper() == b['item'].upper():
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
                        # for item_in_balance in i['balance']:
                        #     if item_in_balance['item'] == a['item'].upper():
                        #         out_data['Total'].append(item_in_balance['qty'])
                        # for item_in_main in i['main']:
                        #     if item_in_main['item'] == a['item'].upper():
                        #         out_data['Main'].append(item_in_main['qty'])
                        # for item_in_ruops in i['ruops']:
                        #     if item_in_ruops['item'] == a['item'].upper():
                        #         out_data['Ru Ops'].append(item_in_ruops['qty'])

    for k, v in updated_store.items():
        store_data['Item'].append(k)
        store_data['Quantity'].append(v)              
        
    return out_data, store_data


def bg_header(x):
    return "background-color: #9ccc65"


def get_text_color(val):
    color = 'red' if val == 'Need to order' else 'black'
    return 'color: %s' % color


def get_center_text(val):
    return 'text-align: center'
        
        
def output_data(all_data, store_data):

    out_path = pathlib.Path('can_collect_redress_kits.xlsx')
    with pd.ExcelWriter('can_collect_redress_kits.xlsx', engine="openpyxl", mode='a', if_sheet_exists="replace") if out_path.exists() else pd.ExcelWriter('can_collect_redress_kits.xlsx', engine="openpyxl", mode='w') as wb:
        df = pd.DataFrame(all_data)
        df_store = pd.DataFrame(store_data)
        SHEETNAME = 'Collect Redress Kit'
        SHEETNAME_STORE = 'Store'
  
        df.style.applymap_index(bg_header, axis=1).applymap_index(get_text_color, axis=1).applymap_index(get_center_text, axis=1).set_properties(**{'text-align': 'center'}).to_excel(wb, sheet_name=SHEETNAME, index=False)
        df_store.to_excel(wb, sheet_name=SHEETNAME_STORE, index=False)
        df_store.style.applymap_index(bg_header, axis=1).applymap_index(get_text_color, axis=1).applymap_index(get_center_text, axis=1).set_properties(**{'text-align': 'center'}).to_excel(wb, sheet_name=SHEETNAME_STORE, index=False)
            
        ws = wb.sheets[SHEETNAME]
        ws.auto_filter.ref='a:g'
        fr = ws['A1']
        
        ws.column_dimensions['A'].width = 13
        ws.column_dimensions['B'].width = 8
        ws.column_dimensions['C'].width = 10
        ws.column_dimensions['D'].width = 10
        ws.column_dimensions['E'].width = 12
        ws.column_dimensions['F'].width = 8
        ws.column_dimensions['G'].width = 30
        ws.column_dimensions['H'].width = 10
        ws.column_dimensions['i'].width = 10
        ws.column_dimensions['j'].width = 10
        ws.column_dimensions['k'].width = 10
        #ws.column_dimensions['l'].width = 10
        ws['A1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['B1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['C1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['D1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['E1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['F1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['G1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['H1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['I1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['J1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['K1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        #ws['L1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws.freeze_panes = fr   

        ws_store = wb.sheets[SHEETNAME_STORE]
        ws_store.auto_filter.ref='a:b'
        ws_store.column_dimensions['A'].width = 20
        ws_store.column_dimensions['B'].width = 11
        ws['A1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        ws['B1'].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
            
            
def main():
    items_by_stock, total, serial_items = get_data_from_excel_stock(FILE_PATH)
    #print(serial_items)
    required_redress_kits = get_data_from_excel_required_redress(FILE_PATH)
    redress_kit_bom = get_data_from_excel_redress_kits_bom(FILE_PATH)
    required_with_items = merge_consist(required_redress_kits, redress_kit_bom)
    data, updated_store = merge_store(total, required_with_items, serial_items)
    print(data)
    all_data, store_data = handling_data(data, updated_store, items_by_stock)
    output_data(all_data, store_data)
    
    
    
if __name__ == '__main__':
    main()