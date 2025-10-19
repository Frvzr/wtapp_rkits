from typing import Dict, Tuple
import pandas as pd
from math import floor

from constants import (
    INPUT_FILE, FILE_PATH, SHEETNAME_INPUT_BOM, SHEETNAME_INPUT_STOCK, SHEETNAME_INPUT_REQUIRED
)
from logger import get_logger


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
        missing_in_stock = [item for item in items if item not in actual_stock['Part Number'].values]
        missing_items_df = pd.DataFrame({'Part Number': missing_in_stock})

        # добавление в DataFrame отсутствующих частей
        actual_stock_ = pd.concat([
            actual_stock,
            missing_items_df
        ], ignore_index=True).fillna({'SN': '', 'Total': 0})

        # УДАЛЕНИЕ ДУБЛИКАТОВ по Part Number и SN
        print(f"До удаления дубликатов: {len(actual_stock_)} строк")

        # Проверяем наличие дубликатов
        duplicates = actual_stock_[actual_stock_.duplicated(subset=['Part Number', 'SN'], keep=False)]
        if not duplicates.empty:
            print(f"Найдены дубликаты по Part Number и SN:")
            print(duplicates[['Part Number', 'SN']].to_string())

        # Удаляем дубликаты, оставляя первую запись
        actual_stock_clean = actual_stock_.drop_duplicates(subset=['Part Number', 'SN'], keep='first')

        print(f"После удаления дубликатов: {len(actual_stock_clean)} строк")

        return actual_stock_clean

    except ValueError as ve:
        logger.critical(f"Value error in {INPUT_FILE}: {str(ve)}", exc_info=True)
        raise
    except KeyError as ke:
        logger.critical(f"Column error in {INPUT_FILE}: {str(ke)}", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"Unexpected error processing {INPUT_FILE}: {str(e)}", exc_info=True)
        raise

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
            seen_combinations = set()

            for index, row in qty_on_store_data.iterrows():
                if row['Part Number'] == item_key:
                    combination_key = (row['Part Number'], row['SN'])
                    if combination_key not in seen_combinations:
                        serial['serial_items'].append({
                            'sn_item': row['Part Number'],
                            'serial_number': row['SN'],
                            'sn_qty': row['Total']
                        })
                        seen_combinations.add(combination_key)

        # Резервирование
        reserved = get_reserved(required, item, qty_on_store_data)
        print("Reserved data:", reserved)

        # Обновление данных склада
        qty_on_store_data = update_store(qty_on_store_data, reserved)

        # Формирование итоговых данных
        max_collect_redress['maximum collect rkits'].append({
            'redress_kit': item['redress_kit'],
            "total": item["total"],
            "consist": item["consist"],
            "max_collect_items": max_collect_items["max_collect_items"],
            'qty_on_store': qty_on_store['qty_on_store'],
            'reserved': reserved['update_data'],
            'serial': serial['serial_items']
        })
    print(max_collect_redress['maximum collect rkits'])
    logger.info("Merging data with stock completed")
    return max_collect_redress, qty_on_store_data


def get_reserved(res: int, redress: Dict, qty_on_store_data: pd.DataFrame) -> Dict:
    """Расчет резервирования items с приоритетом по наименьшему серийному номеру"""
    reserved = {'reserved': [], 'update_data': []}
    required = res if res >= 0 else 1

    for item in redress['consist']:
        item_from_consist = item['item'].upper()
        qty_from_consist = item['qty']

        # Получаем все записи для данного Part Number
        part_data = qty_on_store_data[qty_on_store_data['Part Number'] == item_from_consist].copy()
        qty_from_store = part_data['Total'].sum()

        if qty_from_store > 0:
            req = qty_from_consist * int(required)
            reserv = qty_from_store - req
            reserved_qty = req if reserv >= 0 else qty_from_store
        else:
            reserved_qty = 0

        # Добавляем в reserved
        reserved['reserved'].append({
            "item": item_from_consist,
            "qty": reserved_qty
        })

        # Вычисляем детальные данные для обновления склада
        if reserved_qty > 0 and not part_data.empty:
            remaining_to_subtract = reserved_qty

            # СОРТИРУЕМ ПО НАИМЕНЬШЕМУ СЕРИЙНОМУ НОМЕРУ
            # Сначала разделяем записи с SN и без SN
            with_sn = part_data[part_data['SN'].apply(lambda x: bool(str(x).strip()) if pd.notna(x) else False)].copy()
            without_sn = part_data[
                part_data['SN'].apply(lambda x: not bool(str(x).strip()) if pd.notna(x) else True)].copy()

            # Сортируем записи с SN по возрастанию серийного номера (наименьшие первые)
            with_sn = with_sn.sort_values('SN', ascending=True)

            # Объединяем: сначала записи с SN (отсортированные по возрастанию), затем без SN
            sorted_data = pd.concat([with_sn, without_sn])

            print(f"Распределение для {item_from_consist}: требуется {reserved_qty}")

            # ВАЖНО: сначала обрабатываем ВСЕ записи для вычитания
            processed_records = []

            for _, row in sorted_data.iterrows():
                current_total = row['Total']
                current_sn = row['SN']

                if remaining_to_subtract <= 0:
                    # Если уже всё вычли, сохраняем исходное количество
                    processed_records.append({
                        'Part Number': item_from_consist,
                        'Serial Number': current_sn,
                        'Qty': current_total
                    })
                    continue

                print(f"  SN {current_sn}: доступно {current_total}, нужно вычесть {remaining_to_subtract}")

                if current_total >= remaining_to_subtract:
                    # Вычитаем полностью из текущей записи
                    new_qty = current_total - remaining_to_subtract
                    processed_records.append({
                        'Part Number': item_from_consist,
                        'Serial Number': current_sn,
                        'Qty': new_qty
                    })
                    print(f"    → Вычитаем {remaining_to_subtract}, остаток: {new_qty}")
                    remaining_to_subtract = 0
                else:
                    # Вычитаем всё из текущей записи и переносим остаток
                    processed_records.append({
                        'Part Number': item_from_consist,
                        'Serial Number': current_sn,
                        'Qty': 0
                    })
                    print(f"    → Вычитаем всё ({current_total}), переносим остаток: {remaining_to_subtract - current_total}")
                    remaining_to_subtract -= current_total

            # Добавляем ВСЕ обработанные записи в update_data
            reserved['update_data'].extend(processed_records)

        elif not part_data.empty:
            # Если нет резервирования, добавляем все записи без изменений
            for _, row in part_data.iterrows():
                reserved['update_data'].append({
                    'Part Number': item_from_consist,
                    'Serial Number': row['SN'],
                    'Qty': row['Total']
                })
    print("UPDATE_DATA", reserved['update_data'])
    return reserved


def get_min_data(data: Dict) -> int:
    """Получение минимального значения из данных"""
    min_data = [i['qty'] for i in data['max_collect_items']] if data['max_collect_items'] else [0]
    return min(min_data)


def update_store(qty_on_store_data: pd.DataFrame, reserved: Dict) -> pd.DataFrame:
    """Обновление данных склада после резервирования"""
    if 'update_data' not in reserved:
        print("Ошибка: неверный формат reserved данных")
        return qty_on_store_data

    # Создаем копию DataFrame для безопасного обновления
    updated_data = qty_on_store_data.copy()

    # Создаем словарь для быстрого доступа к обновлениям
    update_dict = {}
    for update_item in reserved['update_data']:
        key = (update_item['Part Number'], update_item['Serial Number'])
        update_dict[key] = update_item['Qty']

    # Применяем обновления
    for idx, row in updated_data.iterrows():
        key = (row['Part Number'], row['SN'])
        if key in update_dict:
            updated_data.at[idx, 'Total'] = update_dict[key]

    print(f"Обновлено {len(update_dict)} записей на складе")
    return updated_data


def process_redress_data(input_data, output_file='redress_kits_report.xlsx'):
    """
    Обрабатывает данные о redress kits и создает Excel отчет
    с использованием реальных данных о резервировании из update_data
    """
    # Создаем списки для хранения данных
    all_rows = []

    # Проходим по всем redress kits
    for kit_type, kits in input_data.items():
        for kit in kits:
            redress_kit = kit['redress_kit']
            qty_on_store = kit['total'][0]['q-ty on store']
            required = kit['total'][0]['required']

            # Создаем словари для быстрого доступа к данным
            max_collect_dict = {item['item']: item['qty'] for item in kit['max_collect_items']}
            qty_store_dict = {item['item']: item['qty'] for item in kit['qty_on_store']}

            # Создаем словарь зарезервированных количеств из update_data
            reserved_dict = {}
            for reserved_item in kit.get('reserved', []):
                key = (reserved_item['Part Number'], reserved_item['Serial Number'])
                reserved_dict[key] = reserved_item['Qty']

            # Создаем словарь исходных количеств из serial данных
            original_qty_dict = {}
            for serial_item in kit['serial']:
                key = (serial_item['sn_item'], serial_item['serial_number'])
                original_qty_dict[key] = serial_item['sn_qty']

            # Проходим по всем компонентам kit'а
            for component in kit['consist']:
                item = component['item']
                description = component['description']
                qty_per_kit = component['qty']

                # Вычисляем Need to order
                total_needed = required * qty_per_kit
                total_available = qty_store_dict.get(item, 0)
                need_to_order = max(0, total_needed - total_available)

                # Получаем все serial numbers для этого item
                item_serials = [s for s in kit['serial'] if s['sn_item'].upper() == item]

                if item_serials:
                    # Для каждого serial number этого item
                    for serial in item_serials:
                        serial_number = serial['serial_number']
                        original_qty = serial['sn_qty']

                        # Получаем зарезервированное количество из update_data
                        reserved_key = (item, serial_number)
                        reserved_qty_in_update = reserved_dict.get(reserved_key, original_qty)

                        # Вычисляем фактически зарезервированное количество
                        # Это разница между исходным количеством и количеством после резервирования
                        actual_reserved = original_qty - reserved_qty_in_update

                        # Вычисляем остаток после резерва
                        remaining_after = reserved_qty_in_update

                        row = {
                            'Redress Kit': redress_kit,
                            'Qty on store': qty_on_store,
                            'Required': required,
                            'Item': item,
                            'Qty per kit': qty_per_kit,
                            'Description': description,
                            'Need to order': need_to_order,
                            'Reserved': actual_reserved,
                            'Serial Number': serial_number,
                            'Main': original_qty,
                            'Remaining After Reserve': remaining_after
                        }
                        all_rows.append(row)
                else:
                    # Если нет serial numbers
                    original_qty = qty_store_dict.get(item, 0)
                    reserved_key = (item, '')
                    reserved_qty = reserved_dict.get(reserved_key, original_qty)
                    actual_reserved = original_qty - reserved_qty

                    row = {
                        'Redress Kit': redress_kit,
                        'Qty on store': qty_on_store,
                        'Required': required,
                        'Item': item,
                        'Qty per kit': qty_per_kit,
                        'Description': description,
                        'Need to order': need_to_order,
                        'Reserved': actual_reserved,
                        'Serial Number': '',
                        'Main': original_qty,
                        'Remaining After Reserve': reserved_qty
                    }
                    all_rows.append(row)

    # Создаем DataFrame
    df = pd.DataFrame(all_rows)

    # Сортируем данные
    df = df.sort_values(['Redress Kit', 'Item', 'Serial Number'])

    # Сохраняем в Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Redress Kits', index=False)

        # Форматирование
        workbook = writer.book
        worksheet = writer.sheets['Redress Kits']

        # Устанавливаем ширину колонок
        column_widths = {
            'A': 15, 'B': 12, 'C': 10, 'D': 12, 'E': 12,
            'F': 30, 'G': 15, 'H': 10, 'I': 15, 'J': 10, 'K': 18
        }

        for col, width in column_widths.items():
            worksheet.column_dimensions[col].width = width

        # Добавляем фильтры
        worksheet.auto_filter.ref = worksheet.dimensions

    print(f"Отчет сохранен в файл: {output_file}")
    return df


def main():
    """Основная функция программы"""
    try:
        logger.info("Starting data processing")

        # Получение данных
        required_redress_kits, required_redress = get_data_from_excel_sheet_required_redress(FILE_PATH)
        redress_kit_bom, items = get_data_from_excel_sheet_redress_kits_bom(FILE_PATH, required_redress)
        items_by_stock = get_data_from_excel_sheet_stock(FILE_PATH, items)
        print(items_by_stock.loc[items_by_stock['Part Number'] == "D206"])
        print(items_by_stock.loc[items_by_stock['Part Number'] == "T006"])
        # Обработка данных
        required_with_items = merge_consist(required_redress_kits, redress_kit_bom)
        data, updated_store = merge_store(required_with_items, items_by_stock)
        print(data)
        df = process_redress_data(data, 'test_report.xlsx')

        logger.info("Program completed successfully")

    except FileNotFoundError as fnf:
        logger.critical(f"File not found: {INPUT_FILE}")
    except Exception as e:
        logger.critical(f"Program failed: {str(e)}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
