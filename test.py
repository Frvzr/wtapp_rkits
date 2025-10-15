from pandas.core.methods.describe import DataFrameDescriber

from constants import (
    INPUT_FILE, FILE_PATH, SHEETNAME_OUTPUT, SHEETNAME_STORE_OUTPUT,
    SHEETNAME_INPUT_BOM, SHEETNAME_INPUT_STOCK, SHEETNAME_INPUT_REQUIRED,
    OUTPUT_FILE
)
import pandas as pd
from math import floor
from pathlib import Path
from openpyxl.styles import Alignment
from openpyxl import load_workbook
from logger import get_logger

from typing import Dict, List, Tuple, Union

logger = get_logger(__name__)


def get_data_from_excel_sheet_required_redress(path: str):
    redress = pd.read_excel(
        path,
        sheet_name=SHEETNAME_INPUT_REQUIRED,
        engine='openpyxl'
    )
    required_redress_kits = {"Required redress kit": []}
    required_redress = redress['Redress kit'].dropna().str.upper()
    for redress_kit, req_qty in redress.groupby("Redress kit", sort=False):
        kit_data = {
            "redress_kit": redress_kit.upper(),
            "total": [
                {
                    "q-ty on store": row["Q-ty on store"],
                    "required": row["Req qty"]
                }
                for _, row in req_qty.iterrows()
            ]
        }
        required_redress_kits["Required redress kit"].append(kit_data)

    logger.info(f"Data from {SHEETNAME_INPUT_REQUIRED} uploaded successfully")

    return required_redress_kits, required_redress


def get_data_from_excel_sheet_redress_kits_bom(path: str, required_redress):
    """Получение данных BOM из Excel"""
    try:
        rk_bom = pd.read_excel(
            path,
            sheet_name=SHEETNAME_INPUT_BOM,
            engine='openpyxl'
        )
        redress_kit_bom = {"redress kit consist": []}
        f_rk_bom = rk_bom[rk_bom["Redress Part Number"].str.upper().isin(required_redress)]
        items = f_rk_bom['Item Part Number'].dropna().str.upper()
        for redress_kit_, redress_kit_items in f_rk_bom.groupby("Redress Part Number"):
            kit_data = {
                "redress kit": redress_kit_.upper(),
                "consist": [
                    {
                        'item': row["Item Part Number"],
                        'description': row['Description'],
                        'qty': row["Quantity pr."]
                    }
                    for _, row in redress_kit_items.iterrows()
                    if row["Quantity pr."] not in (0, '0')
                ]
            }
            redress_kit_bom["redress kit consist"].append(kit_data)

        logger.info(f"Data from {SHEETNAME_INPUT_BOM} uploaded successfully")
        return redress_kit_bom, items

    except Exception as e:
        logger.critical(f"Error reading BOM data: {str(e)}", exc_info=True)
        raise

def get_data_from_excel_sheet_stock(path: str, items: list):
    """
    Получение данных о запасах из Excel файла

    Args:
        path: Путь к файлу Excel

    Returns:
        Кортеж с данными:
        - items_by_stock: Данные по складам
        - total: Общие количества
        - actual_serial_items: Серийные номера
    """
    try:
        logger.info('Starting program')

        # Чтение данных с явным указанием engine
        stock_dataframe = pd.read_excel(
            path,
            sheet_name=SHEETNAME_INPUT_STOCK,
            engine='openpyxl'
        )

        actual_stock = stock_dataframe[stock_dataframe['Part Number'].isin(items)]

        missing_items = pd.DataFrame({'Part Number': items})

        # добавление в DataFrame отсутствующих частей
        actual_stock_ = missing_items.merge(
            actual_stock,
            on='Part Number',
            how='left'
        ).fillna({'SN': '', 'Total': 0})

        return actual_stock_

    except ValueError as ve:
        logger.critical(f"Value error in {INPUT_FILE}: {str(ve)}", exc_info=True)
        raise
    except KeyError as ke:
        logger.critical(f"Column error in {INPUT_FILE}: {str(ke)}", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"Unexpected error processing {INPUT_FILE}: {str(e)}", exc_info=True)
        raise


def merge_consist(required_redress_kits: Dict, redress_kit_bom: Dict) -> Dict:
    """Объединение данных о требуемых наборах и BOM"""
    required_with_items = {'Items for redress kits': []}

    for req_kit in required_redress_kits['Required redress kit']:
        for consist in redress_kit_bom["redress kit consist"]:
            if consist["redress kit"] == req_kit['redress_kit']:
                required_with_items['Items for redress kits'].append({
                    'redress_kit': consist['redress kit'],
                    "total": req_kit["total"],
                    "consist": consist["consist"]
                })

    logger.info("Merging required redress kits and BOM completed")
    return required_with_items


def merge_store(required_with_items: Dict, qty_on_store_data) -> Tuple[Dict, Dict]:
    """Объединение данных со складом"""
    max_collect_redress = {'maximum collect rkits': []}


    for item in required_with_items['Items for redress kits']:
        required = item['total'][-1]['required']
        qty_on_store = {'qty_on_store': []}
        max_collect_items = {'max_collect_items': []}
        reserved = {'reserved': []}
        serial = {'serial_items': []}

        for y in item['consist']:
            item_key = y['item'].upper()
            sum_item = qty_on_store_data.query('`Part Number` == @item_key')['Total'].sum()
            # Расчет максимального количества
            max_collect_item = floor(sum_item / y['qty'])
            max_collect_items['max_collect_items'].append({
                "item": item_key,
                "qty": max_collect_item
            })

            qty_on_store['qty_on_store'].append({
                'item': item_key,
                'qty': float(sum_item)
            })

            # Обработка серийных номеров
            for index, row in qty_on_store_data.iterrows():
                if row['Part Number'] == item_key:

                    serial['serial_items'].append({
                        'sn_item': row['Part Number'],
                        'serial_number': row['SN'],
                        'sn_qty': row['Total']
                    })

        # Расчет минимального количества
        # res = get_min_data(max_collect_items)
        # if not pd.isna(required) and res > required:
        #     res = required
        # Резервирование товаров
        reserved = get_reserved(required, item, qty_on_store_data)
        # Обновление данных склада
        qty_on_store_data = update_store(qty_on_store_data, reserved)
        # Формирование итоговых данных
        max_collect_redress['maximum collect rkits'].append({
            'redress_kit': item['redress_kit'],
            "total": item["total"],
            "consist": item["consist"],
            "max_collect_items": max_collect_items["max_collect_items"],
            'qty_on_store': qty_on_store['qty_on_store'],
            'reserved': reserved['reserved'],
            'serial': serial['serial_items']
        })

    logger.info("Merging data with stock completed")
    return max_collect_redress, qty_on_store_data


def get_reserved(res: int, redress: Dict, qty_on_store_data: Dict) -> Dict:
    """Расчет резервирования items"""
    reserved = {'reserved': []}
    required = res if res >= 0 else 1
    for item in redress['consist']:
        item_from_consist = item['item'].upper()
        qty_from_consist = item['qty']
        qty_from_store = qty_on_store_data.query('`Part Number` == @item_from_consist')['Total'].sum()
        if qty_from_store > 0:
            req = qty_from_consist * int(required)
            reserv = qty_from_store - req
            reserved_qty = req if reserv >= 0 else qty_from_store
        else:
            reserved_qty = 0

        reserved['reserved'].append({
            "item": item_from_consist,
            "qty": reserved_qty
        })
    print(reserved)
    return reserved


def get_min_data(data: Dict) -> int:
    """Получение минимального значения из данных"""
    min_data = [i['qty'] for i in data['max_collect_items']] if data['max_collect_items'] else [0]
    return min(min_data)


def update_store(qty_on_store_data: Dict, reserved: Dict) -> Dict:
    """Обновление данных склада после резервирования"""
    print(reserved)
    if 'reserved' not in reserved:
        print("Ошибка: неверный формат reserved данных")
        return qty_on_store_data

    reserved_items = reserved['reserved']

    for item_data in reserved_items:
        part_number = item_data['item']
        quantity_to_subtract = item_data['qty']

        # Пропускаем нулевые количества
        if quantity_to_subtract <= 0:
            continue

        # Фильтруем записи по Part Number
        part_qty_on_store_data = qty_on_store_data[qty_on_store_data['Part Number'] == part_number].copy()

        if part_qty_on_store_data.empty:
            print(f"Part Number {part_number} не найден, пропускаем")
            continue

        # Сортируем: сначала с SN (по убыванию), затем по Total (по убыванию)
        part_qty_on_store_data['has_sn'] = part_qty_on_store_data['SN'].apply(lambda x: 1 if x and str(x).strip() else 0)
        #part_qty_on_store_data = part_qty_on_store_data.sort_values(['has_sn', 'Total'], ascending=[False, False])

        # print(f"\nОтсортированные данные для вычитания {part_number}:")
        # print(part_qty_on_store_data[['Part Number', 'SN', 'Total']])

        remaining_to_subtract = quantity_to_subtract

        # Проходим по всем записям и вычитаем количество
        for index, row in part_qty_on_store_data.iterrows():
            if remaining_to_subtract <= 0:
                break

            current_total = row['Total']

            if current_total >= remaining_to_subtract:
                # Вычитаем полностью из текущей записи
                qty_on_store_data.loc[index, 'Total'] = current_total - remaining_to_subtract
                remaining_to_subtract = 0
            else:
                # Вычитаем всё из текущей записи и переносим остаток
                qty_on_store_data.loc[index, 'Total'] = 0
                remaining_to_subtract -= current_total

        if remaining_to_subtract > 0:
            print(f"Внимание: для {part_number} не хватило {remaining_to_subtract} единиц для вычитания")
        else:
            print(f"Успешно вычли {quantity_to_subtract} для {part_number}")

    return qty_on_store_data

    # for j in reserved:
    #     item_key = j['item'].upper()
    #     result = qty_on_store_data.query('`Part Number` == @item_key')
    #     if item_key in qty_on_store_data:
    #         new_qty = qty_on_store_data[item_key] - j['qty']
    #         qty_on_store_data[item_key] = max(new_qty, 0)
    # print(qty_on_store_data)
    # return qty_on_store_data


def main():
    """Основная функция программы"""
    try:
        logger.info("Starting data processing")

        # Получение данных

        required_redress_kits, required_redress = get_data_from_excel_sheet_required_redress(FILE_PATH)
        redress_kit_bom, items = get_data_from_excel_sheet_redress_kits_bom(FILE_PATH, required_redress)
        items_by_stock = get_data_from_excel_sheet_stock(FILE_PATH, items)
        # Обработка данных
        required_with_items = merge_consist(required_redress_kits, redress_kit_bom)
        data, updated_store = merge_store(required_with_items, items_by_stock)
        print(data)
        print(updated_store)
        # all_data = data_handling_items(data, updated_store)
        # store_data = data_handling_store(updated_store)
        #
        # # Сохранение результатов
        # output_data(all_data, store_data)

        logger.info("Program completed successfully")

    except FileNotFoundError as fnf:
        logger.critical(f"File not found: {INPUT_FILE}")
    except Exception as e:
        logger.critical(f"Program failed: {str(e)}", exc_info=True)
        raise


if __name__ == '__main__':
    main()