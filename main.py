import pandas as pd

path = 'C:\\Users\\user\\Desktop\\Redress\\test_file.xlsx'


redress = pd.read_excel(path, sheet_name='Redress')
print(redress)

rk_bom = pd.read_excel(path, sheet_name='redress_kits_items')
dt = {"series": []}
for i, g in rk_bom.groupby("Redress Part Number"):
    dt["series"].append({"redress kit": i, "consist": []})
    for w, s in zip(g["Item Part Number"], g["Quantity pr."]):
        dt["series"][-1]["consist"].append({'item': w, 'qty': s})
#print(dt)

qty_on_store = pd.read_excel(path, sheet_name='Pivot Stock')
qty_on_store_dict = dict(zip(qty_on_store['Row Labels'], qty_on_store['Sum of QTY']))
#print(qty_on_store_dict)
