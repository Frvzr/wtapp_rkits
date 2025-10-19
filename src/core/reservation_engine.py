from typing import Dict
import pandas as pd
from .data_models import ReservationResult, StockItem, ReservationStrategy
import logging

logger = logging.getLogger(__name__)


class ReservationEngine:
    def __init__(self, strategy: ReservationStrategy = ReservationStrategy.LOWEST_SN_FIRST):
        self.strategy = strategy

    def reserve_items(self, res: int, redress: Dict, qty_on_store_data: pd.DataFrame) -> ReservationResult:
        """Основная логика резервирования"""
        #logger.info(f"Starting reservation with strategy: {self.strategy}")

        if self.strategy == ReservationStrategy.LOWEST_SN_FIRST:
            return self._reserve_lowest_sn_first(res, redress, qty_on_store_data)
        elif self.strategy == ReservationStrategy.HIGHEST_QTY_FIRST:
            return self._reserve_highest_qty_first(res, redress, qty_on_store_data)
        else:
            raise ValueError(f"Unknown reservation strategy: {self.strategy}")

    #     )
    def _reserve_lowest_sn_first(self, res: int, redress: Dict, qty_on_store_data: pd.DataFrame) -> ReservationResult:
        """Резервируем от меньшего SN большему, оригинальные в конце"""
        reserved = {'reserved': [], 'update_data': []}
        required = res if res >= 0 else 1

        #print(f"\n🎯 RESERVATION START: {redress.get('redress_kit', 'Unknown')}, required kits={required}")

        for item in redress['consist']:
            item_from_consist = item['item'].upper()
            qty_from_consist = item['qty']
            total_needed = qty_from_consist * required

            #print(f"\n🔍 Processing: {item_from_consist}, qty_per_kit={qty_from_consist}, total_needed={total_needed}")

            # Получаем все записи для данного Part Number
            part_data = qty_on_store_data[qty_on_store_data['Part Number'] == item_from_consist].copy()
            total_available = part_data['Total'].sum()

            #print(f"📊 Available: {total_available} across {len(part_data)} records")

            # Расчет сколько нужно зарезервировать
            if total_available > 0:
                req = qty_from_consist * int(required)
                reserved_qty = min(req, total_available)
            else:
                reserved_qty = 0

            # Добавляем в reserved
            reserved['reserved'].append({
                "item": item_from_consist,
                "qty": reserved_qty
            })

            #print(f"💰 Reserved qty: {reserved_qty}")

            # ВЫЧИСЛЯЕМ ДЕТАЛЬНЫЕ ДАННЫЕ ДЛЯ ОБНОВЛЕНИЯ СКЛАДА
            if reserved_qty > 0 and not part_data.empty:
                remaining_to_subtract = reserved_qty
                #print(f"   🧮 Need to subtract: {remaining_to_subtract} from stock")

                # СОРТИРУЕМ по наименьшему SN
                with_sn = part_data[part_data['SN'].apply(lambda x: bool(str(x).strip()) if pd.notna(x) else False)].copy()
                without_sn = part_data[part_data['SN'].apply(lambda x: not bool(str(x).strip()) if pd.notna(x) else True)].copy()
                with_sn = with_sn.sort_values('SN', ascending=True)
                sorted_data = pd.concat([with_sn, without_sn])

                processed_records = []

                for i, (_, row) in enumerate(sorted_data.iterrows()):
                    current_total = row['Total']
                    current_sn = row['SN']

                    # Конвертируем SN в строку
                    if pd.isna(current_sn) or current_sn == '':
                        current_sn_str = ""
                    elif isinstance(current_sn, (int, float)):
                        if isinstance(current_sn, float) and current_sn.is_integer():
                            current_sn_str = str(int(current_sn))
                        else:
                            current_sn_str = str(current_sn)
                    else:
                        current_sn_str = str(current_sn).strip()

                    #print(f"   📦 Record {i+1}: SN='{current_sn_str}', Available={current_total}, Remaining to subtract={remaining_to_subtract}")

                    if remaining_to_subtract <= 0:
                        # Если уже вычли всё нужное, сохраняем исходное количество
                        if current_total > 0:
                            processed_records.append(StockItem(
                                part_number=item_from_consist,
                                serial_number=current_sn_str,
                                total=current_total
                            ))
                            #print(f"      ✅ No more needed, keep {current_total}")
                        continue

                    if current_total > 0:
                        if current_total >= remaining_to_subtract:
                            # Берем нужное количество из этой записи
                            new_qty = current_total - remaining_to_subtract
                            processed_records.append(StockItem(
                                part_number=item_from_consist,
                                serial_number=current_sn_str,
                                total=new_qty
                            ))
                            #print(f"      ✅ Take {remaining_to_subtract} from this SN, remaining: {new_qty}")
                            remaining_to_subtract = 0
                        else:
                            # Берем всё из этой записи
                            processed_records.append(StockItem(
                                part_number=item_from_consist,
                                serial_number=current_sn_str,
                                total=0
                            ))
                            #print(f"      ✅ Take all {current_total} from this SN")
                            remaining_to_subtract -= current_total
                    else:
                        # Запись с нулевым количеством - всё равно добавляем
                        processed_records.append(StockItem(
                            part_number=item_from_consist,
                            serial_number=current_sn_str,
                            total=0
                        ))
                        #print(f"      ⚠️  Zero quantity, skip")

                # Добавляем ВСЕ обработанные записи в update_data
                reserved['update_data'].extend(processed_records)
                #print(f"📋 Added {len(processed_records)} records to update_data")

            elif not part_data.empty:
                # Если резервирования нет, но есть данные - сохраняем ВСЕ записи как есть
                #print(f"📋 No reservation needed, keeping {len(part_data)} records as-is")
                for _, row in part_data.iterrows():
                    current_sn = row['SN']
                    if pd.isna(current_sn) or current_sn == '':
                        current_sn_str = ""
                    elif isinstance(current_sn, (int, float)):
                        if isinstance(current_sn, float) and current_sn.is_integer():
                            current_sn_str = str(int(current_sn))
                        else:
                            current_sn_str = str(current_sn)
                    else:
                        current_sn_str = str(current_sn).strip()

                    reserved['update_data'].append(StockItem(
                        part_number=item_from_consist,
                        serial_number=current_sn_str,
                        total=row['Total']
                    ))
            else:
                print(f"📋 No data for item {item_from_consist}")

        #print(f"\n🎯 RESERVATION COMPLETE: {len(reserved['update_data'])} total update records, {len(reserved['reserved'])} items reserved")

        return ReservationResult(
            reserved=reserved['reserved'],
            update_data=reserved['update_data']
        )

    def _reserve_highest_qty_first(self, res: int, redress: Dict, qty_on_store_data: pd.DataFrame) -> ReservationResult:
        """Стратегия: наибольшее количество первый"""
        # TODO: Реализация стратегии наибольшего количества
        return self._reserve_lowest_sn_first(res, redress, qty_on_store_data)

    def validate_reservation(self, reservation: ReservationResult) -> bool:
        """Валидация результатов резервирования"""
        if not reservation.reserved or not reservation.update_data:
            logger.error("Invalid reservation: empty data")
            return False

        total_reserved = sum(item['qty'] for item in reservation.reserved)
        total_updated = sum(item.total for item in reservation.update_data)

        # Базовая проверка корректности данных
        if total_reserved < 0 or total_updated < 0:
            logger.error("Invalid reservation: negative quantities")
            return False

        logger.info(f"Reservation validated: {total_reserved} units reserved")
        return True
