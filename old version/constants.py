from os import getcwd

DIR = getcwd()
INPUT_FILE = '../required_redress_kit.xlsx'
FILE_PATH = f'{DIR}\\{INPUT_FILE}'
#SHEETNAME_INPUT_STOCK = 'Stock 2023'
SHEETNAME_INPUT_STOCK = 'StockIMC2023'
SHEETNAME_INPUT_REQUIRED = 'Required redress kits'
SHEETNAME_INPUT_BOM = 'Redress kit BOM'

OUTPUT_FILE = "../can_collect_redress_kits.xlsx"
SHEETNAME_OUTPUT = 'Collect Redress Kit'
SHEETNAME_STORE_OUTPUT = 'Updated stocks'