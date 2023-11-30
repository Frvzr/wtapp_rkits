import os
import pandas as pd
from math import floor


DIR = os.getcwd()
FILE_PATH_TEST = f'{DIR}\\required_redress_kit.xlsx'


def get_data_from_excel(FILE_PATH):

    df = pd.read_excel(FILE_PATH, sheet_name='Stock 2023')
    dataset = {'Main':{}, 'Ru Ops': {}}
    total = {}
    agg_func_math = {'QTY':['sum']}
    data = df.groupby(['Part Number', 'Stock', ]).agg(agg_func_math).to_dict()
    for k, v in data.items():
        for i, j in v.items():
            if i[1] == 'Main':
                dataset['Main'][i[0]] = j
            else:
                dataset['Ru Ops'][i[0]] = j
    #print(dataset)
    for k, v in dataset.items():
        if k in total:
            total[k] += v
        else:
            total[k] = v
    #print(total)
    
    serial_n = df.groupby(['Part Number', 'Stock', 'SN']).agg(agg_func_math).to_dict()
    print(serial_n)
 
def main():
    test_data = get_data_from_excel(FILE_PATH_TEST)
    #print(test_data)


if __name__ == '__main__':
    main()
    