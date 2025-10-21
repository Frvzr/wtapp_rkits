import pandas as pd
from typing import Tuple, Dict, List
import logging
from .reservation_engine import ReservationEngine

logger = logging.getLogger(__name__)


class InventoryManager:
    def __init__(self, reservation_engine: ReservationEngine):
        self.reservation_engine = reservation_engine

    def merge_store_data(self, required_with_items: Dict, qty_on_store_data: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
        """Объединение данных со складом с последовательным обновлением запасов"""
        max_collect_redress = {'maximum collect rkits': []}
        current_stock = qty_on_store_data.copy()

        kits = required_with_items['Items for redress kits']
        #print(f"\n🎯 INVENTORY MANAGER: Processing {len(kits)} kits sequentially")

        for i, item in enumerate(kits):
            required = item['total'][-1]['required']
            qty_on_store = {'qty_on_store': []}
            max_collect_items = {'max_collect_items': []}
            serial = {'serial_items': []}

            #print(f"\n🔷 PROCESSING KIT {i+1}/{len(kits)}: {item['redress_kit']}, required={required}")

            # Проверка доступности с ТЕКУЩИМИ запасами
            availability_status = self._check_availability(item, current_stock)

            # СОХРАНЯЕМ ИСХОДНЫЕ ДАННЫЕ ДЛЯ ОТЧЕТА ДО РЕЗЕРВИРОВАНИЯ
            original_serial_data = {}
            for component in item['consist']:
                item_key = component['item'].upper()
                sum_item = current_stock.query('`Part Number` == @item_key')['Total'].sum()

                # Сохраняем исходные serial данные
                serial_items = self._get_unique_serial_items(item_key, current_stock)
                original_serial_data[item_key] = serial_items.copy()

                # Расчет максимального количества
                max_collect_item = self._calculate_max_collect(sum_item, component['qty'])
                max_collect_items['max_collect_items'].append({
                    "item": item_key,
                    "qty": max_collect_item
                })

                qty_on_store['qty_on_store'].append({
                    'item': item_key,
                    'qty': float(sum_item)
                })

                serial['serial_items'].extend(serial_items)

            # Резервирование
            reserved = self.reservation_engine.reserve_items(required, item, current_stock)

            # Формируем reserved_data для отчета с ИСХОДНЫМИ и ОБНОВЛЕННЫМИ данными
            reserved_data = []
            if reserved and hasattr(reserved, 'update_data'):
                # Обновление данных склада
                current_stock = self._update_stock_data(current_stock, reserved.update_data)

                # Формируем данные для отчета
                for update_item in reserved.update_data:
                    part_number = update_item.part_number
                    serial_number = update_item.serial_number
                    updated_qty = update_item.total

                    # Находим исходное количество из original_serial_data
                    original_qty = 0
                    for serial_item in original_serial_data.get(part_number, []):
                        if serial_item['serial_number'] == serial_number:
                            original_qty = serial_item['sn_qty']
                            break

                    reserved_data.append({
                        'Part Number': part_number,
                        'Serial Number': serial_number,
                        'Qty': updated_qty,  # Остаток после резерва
                        'Original_Qty': original_qty  # Исходное количество
                    })

            # Формирование итоговых данных кита
            kit_data = {
                'redress_kit': item['redress_kit'],
                "total": item["total"],
                "consist": item["consist"],
                "max_collect_items": max_collect_items["max_collect_items"],
                'qty_on_store': qty_on_store['qty_on_store'],
                'reserved': reserved_data,
                'serial': serial['serial_items'],
                'availability_status': availability_status,
                'can_produce': any(item['qty'] > 0 for item in max_collect_items["max_collect_items"])
            }

            max_collect_redress['maximum collect rkits'].append(kit_data)

        return max_collect_redress, current_stock

    def _check_availability(self, item: Dict, stock_data: pd.DataFrame) -> Dict:
        """Проверка доступности компонентов на основе текущих запасов"""
        availability_info = {
            'is_available': True,
            'missing_components': [],
            'component_details': []
        }

        for component in item['consist']:
            component_name = component['item'].upper()
            available = stock_data[
                stock_data['Part Number'] == component_name
                ]['Total'].sum()
            needed = component['qty'] * item['total'][-1]['required']

            component_info = {
                'component': component_name,
                'available': available,
                'needed': needed,
                'is_sufficient': available >= needed,
                'shortage': max(0, needed - available)
            }

            availability_info['component_details'].append(component_info)

            if available < needed:
                availability_info['is_available'] = False
                availability_info['missing_components'].append({
                    'component': component_name,
                    'shortage': needed - available
                })
                logger.warning(f"Component {component_name} insufficient: {available} < {needed}")

        return availability_info

    def _calculate_max_collect(self, qty_on_store: float, qty_per_kit: float) -> int:
        """Расчет максимального количества наборов"""
        from math import floor
        if qty_per_kit == 0:
            return 0
        return floor(qty_on_store / qty_per_kit)

    def _get_unique_serial_items(self, item_key: str, stock_data: pd.DataFrame) -> List[Dict]:
        """Получение уникальных серийных номеров из текущих запасов"""
        seen = set()
        serial_items = []
        for _, row in stock_data[stock_data['Part Number'] == item_key].iterrows():
            key = (row['Part Number'], row['SN'])
            if key not in seen:
                serial_items.append({
                    'sn_item': row['Part Number'],
                    'serial_number': row['SN'],
                    'sn_qty': row['Total']
                })
                seen.add(key)
        return serial_items

    def _update_stock_data(self, stock_data: pd.DataFrame, updates: List) -> pd.DataFrame:
        """Обновление данных склада с правильным сопоставлением форматов SN"""
        updated_data = stock_data.copy()

        update_count = 0
        changes = []

        #print(f"   🔧 UPDATING STOCK: {len(updates)} update records")

        for update_item in updates:
            # Получаем данные из update_item
            if hasattr(update_item, 'part_number'):
                part_number = update_item.part_number
                serial_number = update_item.serial_number
                new_total = update_item.total
            else:
                part_number = update_item.get('Part Number', '')
                serial_number = update_item.get('Serial Number', '')
                new_total = update_item.get('Qty', update_item.get('total', 0))

            # Нормализуем serial_number для сравнения
            if serial_number is None or (isinstance(serial_number, (int, float)) and pd.isna(serial_number)):
                serial_number_norm = ""
            elif isinstance(serial_number, (int, float)):
                # Для числовых SN приводим к целому если возможно
                if isinstance(serial_number, float) and serial_number.is_integer():
                    serial_number_norm = str(int(serial_number))
                else:
                    serial_number_norm = str(serial_number)
            else:
                serial_number_norm = str(serial_number).strip()

            # Ищем соответствующую запись в stock_data
            match_found = False
            for idx, row in updated_data.iterrows():
                stock_part = row['Part Number']
                stock_sn = row['SN']

                # Нормализуем stock_sn для сравнения
                if stock_sn is None or (isinstance(stock_sn, (int, float)) and pd.isna(stock_sn)):
                    stock_sn_norm = ""
                elif isinstance(stock_sn, (int, float)):
                    if isinstance(stock_sn, float) and stock_sn.is_integer():
                        stock_sn_norm = str(int(stock_sn))
                    else:
                        stock_sn_norm = str(stock_sn)
                else:
                    stock_sn_norm = str(stock_sn).strip()

                # Сравниваем нормализованные значения
                if stock_part == part_number and stock_sn_norm == serial_number_norm:
                    old_value = row['Total']
                    if old_value != new_total:
                        updated_data.at[idx, 'Total'] = new_total
                        update_count += 1
                        changes.append(f"{part_number} SN:'{serial_number_norm}' {old_value}→{new_total}")
                        #print(f"      🔄 UPDATED: {part_number} SN:'{serial_number_norm}' {old_value}→{new_total}")
                    match_found = True
                    break

            if not match_found:
                print(f"      ⚠️  NO MATCH: {part_number} SN:'{serial_number_norm}' - record not found in stock")

        #print(f"   📊 UPDATE SUMMARY: {update_count} records modified")
        #ogger.info(f"Stock data updated: {update_count} records modified")
        return updated_data
