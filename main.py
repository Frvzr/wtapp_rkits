import os
import pandas as pd

DIR = os.getcwd()

path = f'{DIR}\\test_file.xlsx'


redress = pd.read_excel(path, sheet_name='Redress')
redress_dict = {"series": []}
for k, v in redress.groupby("Redress kit"):
    redress_dict["series"].append({"redress_kit": k, "total": []})
    for q, r in zip(v["Q-ty on store"], v["Req qty"]):
        redress_dict["series"][-1]["total"].append({"q-ty on store": q, "required": r})
print(redress_dict)

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

nd = {"redress_kit": None,
      "total": [],
      "consist": []}

for k, v in dt.items():
    for i in v:
        for a, b in redress_dict.items():
            for z in b:
                if i["redress kit"] == z['redress_kit']:
                    nd.update({z['redress_kit']:i['redress kit']})
                    

print(nd)