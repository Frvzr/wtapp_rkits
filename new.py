import os
import pandas as pd
from math import floor

DIR = os.getcwd()
FILE_PATH = f'{DIR}\\required_redress_kit.xlsx'


def get_data_from_excel(path):
    dataframe = pd.read_excel(path, sheet_name='Stock 2023')
    data_by_stock = dataframe.groupby(['Part Number', 'Stock']).agg({'QTY': ['sum']}).to_dict()
    #print(data_by_stock)
    
    items_by_stock = {'Main': {}, 'Ru Ops': {}}
    for data in data_by_stock.values():
        for data_from_stock, qty in data.items():
            print(data_from_stock)

    
    print(items_by_stock) 

def main():
    data = get_data_from_excel(FILE_PATH)

if __name__ == '__main__':
    main()