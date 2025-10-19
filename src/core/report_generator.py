import pandas as pd
from typing import Dict, List
from openpyxl.styles import Font, PatternFill, Alignment
import logging

logger = logging.getLogger(__name__)


class ReportGenerator:
    def __init__(self, column_config: Dict):
        self.column_config = column_config

    def generate_redress_report(self, input_data: Dict, output_file: str) -> pd.DataFrame:
        """Генерация отчета"""
        try:
            all_rows = []

            if 'maximum collect rkits' not in input_data:
                logger.error("Invalid input data structure: 'maximum collect rkits' key not found")
                return pd.DataFrame()

            kits = input_data['maximum collect rkits']
            #logger.info(f"Processing {len(kits)} kits for report")

            for kit in kits:
                processed_rows = self._process_kit_data(kit)
                all_rows.extend(processed_rows)
                #logger.debug(f"Processed kit {kit.get('redress_kit', 'Unknown')}: {len(processed_rows)} rows")

            # Создаем DataFrame
            df = self._create_dataframe(all_rows)
            #logger.info(f"Created DataFrame with {len(df)} rows")

            # Сохраняем в Excel
            if not df.empty:
                self._save_to_excel(df, output_file)
                #logger.info(f"Report generated successfully: {output_file}")
            else:
                logger.warning("No data to save to Excel")

            return df

        except Exception as e:
            logger.error(f"Error generating report: {e}")
            raise

    def _process_kit_data(self, kit: Dict) -> List[Dict]:
        """Обработка данных одного набора с расчетом резервирования"""
        rows = []
        try:
            redress_kit = kit['redress_kit']
            qty_on_store = kit['total'][0]['q-ty on store']
            required = kit['total'][0]['required']

            # Создаем словари для быстрого доступа
            qty_store_dict = {item['item']: item['qty'] for item in kit['qty_on_store']}

            reserved_dict = {}
            for reserved_item in kit.get('reserved', []):
                part_number = reserved_item.get('Part Number')
                serial_number = reserved_item.get('Serial Number')
                qty = reserved_item.get('Qty')

                if part_number is not None and serial_number is not None:
                    # Конвертируем serial_number в строку для consistency
                    if pd.isna(serial_number) or serial_number == '':
                        serial_number_str = ""
                    elif isinstance(serial_number, (int, float)):
                        if isinstance(serial_number, float) and serial_number.is_integer():
                            serial_number_str = str(int(serial_number))
                        else:
                            serial_number_str = str(serial_number)
                    else:
                        serial_number_str = str(serial_number).strip()

                    key = (part_number.upper(), serial_number_str)
                    reserved_dict[key] = qty

            # СОЗДАНИЕ original_qty_dict из serial данных
            original_qty_dict = {}
            for serial_item in kit.get('serial', []):
                sn_item = serial_item.get('sn_item')
                serial_number = serial_item.get('serial_number')
                sn_qty = serial_item.get('sn_qty')

                if sn_item is not None and serial_number is not None:
                    # Конвертируем serial_number в строку
                    if pd.isna(serial_number) or serial_number == '':
                        serial_number_str = ""
                    elif isinstance(serial_number, (int, float)):
                        if isinstance(serial_number, float) and serial_number.is_integer():
                            serial_number_str = str(int(serial_number))
                        else:
                            serial_number_str = str(serial_number)
                    else:
                        serial_number_str = str(serial_number).strip()

                    key = (sn_item.upper(), serial_number_str)
                    original_qty_dict[key] = sn_qty

            # Проходим по всем компонентам kit'а
            for component in kit['consist']:
                item = component['item'].upper()
                description = component['description']
                qty_per_kit = component['qty']

                # Вычисляем Need to order
                total_needed = required * qty_per_kit
                total_available = qty_store_dict.get(item, 0)
                need_to_order = max(0, total_needed - total_available)

                # Получаем все serial numbers для этого item
                item_serials = [s for s in kit.get('serial', []) if s.get('sn_item', '').upper() == item]

                if item_serials:
                    # Для каждого serial number этого item
                    for serial in item_serials:
                        serial_number = serial.get('serial_number', '')
                        original_qty = serial.get('sn_qty', 0)

                        # Конвертируем serial_number в строку
                        if pd.isna(serial_number) or serial_number == '':
                            serial_number_str = ""
                        elif isinstance(serial_number, (int, float)):
                            if isinstance(serial_number, float) and serial_number.is_integer():
                                serial_number_str = str(int(serial_number))
                            else:
                                serial_number_str = str(serial_number)
                        else:
                            serial_number_str = str(serial_number).strip()

                        # Получаем зарезервированное количество (остаток после резерва)
                        reserved_key = (item, serial_number_str)
                        reserved_qty_after = reserved_dict.get(reserved_key, original_qty)

                        # ВЫЧИСЛЯЕМ фактически зарезервированное количество
                        # Reserved = сколько взяли из этого SN для производства
                        actual_reserved = original_qty - reserved_qty_after

                        # Remaining After Reserve = что осталось после резервирования
                        remaining_after = reserved_qty_after

                        row = {
                            'Redress Kit': redress_kit,
                            'Qty on store': qty_on_store,
                            'Required': required,
                            'Item': item,
                            'Qty per kit': qty_per_kit,
                            'Description': description,
                            'Need to order': need_to_order,
                            'Reserved': actual_reserved,  # Сколько ВЗЯЛИ из этого SN
                            'Serial Number': serial_number_str,
                            'Main': original_qty,  # Исходное количество ДО резерва
                            'After Reserve': remaining_after  # Что ОСТАЛОСЬ после резерва
                        }
                        rows.append(row)
                else:
                    # Если нет serial numbers
                    original_qty = qty_store_dict.get(item, 0)
                    reserved_key = (item, '')
                    reserved_qty_after = reserved_dict.get(reserved_key, original_qty)
                    actual_reserved = original_qty - reserved_qty_after

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
                        'After Reserve': reserved_qty_after
                    }
                    rows.append(row)

            return rows

        except Exception as e:
            logger.error(f"Error processing kit data: {e}")
            import traceback
            print(f"❌ ERROR in _process_kit_data: {e}")
            print(traceback.format_exc())
            return []

    def _create_dataframe(self, rows: List[Dict]) -> pd.DataFrame:
        """Создание DataFrame с безопасной сортировкой"""
        try:
            df = pd.DataFrame(rows)

            if df.empty:
                return df

            # Получаем реальные названия колонок из данных
            available_columns = set(df.columns)

            # Безопасная сортировка - только по существующим колонкам
            sort_columns = []
            config_sort_columns = [
                self.column_config['sorting']['primary'],
                self.column_config['sorting']['secondary'],
                self.column_config['sorting']['tertiary']
            ]

            for col in config_sort_columns:
                if col in available_columns:
                    sort_columns.append(col)
                else:
                    logger.warning(f"Sorting column '{col}' not found in data. Available columns: {available_columns}")

            #print("Sort columns:", sort_columns)

            if sort_columns:
                # ПРЕОБРАЗУЕМ Serial Number К ЕДИНОМУ ФОРМАТУ ПЕРЕД СОРТИРОВКОЙ
                if 'Serial Number' in df.columns:
                    # Создаем копию для сортировки, где числа преобразованы к float
                    df_sorted = df.copy()

                    def safe_convert(x):
                        if pd.isna(x) or x == '':
                            return float('inf')  # Пустые в конец
                        try:
                            return float(x)
                        except (ValueError, TypeError):
                            return float('inf')  # Нечисловые тоже в конец

                    # Создаем временную числовую колонку для сортировки
                    df_sorted['_sn_numeric'] = df_sorted['Serial Number'].apply(safe_convert)

                    # Сортируем сначала по числовому значению, затем по оригинальному
                    sort_columns_with_sn = []
                    for col in sort_columns:
                        sort_columns_with_sn.append(col)
                        if col == 'Item':
                            sort_columns_with_sn.extend(['_sn_numeric', 'Serial Number'])

                    # Убираем дубликаты
                    sort_columns_with_sn = list(dict.fromkeys(sort_columns_with_sn))

                    df_sorted = df_sorted.sort_values(sort_columns_with_sn)
                    df = df.loc[df_sorted.index]  # Переупорядочиваем оригинальный df

                else:
                    df = df.sort_values(sort_columns)

            else:
                logger.warning("No valid sorting columns found, using default order")

            return df.reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _save_to_excel(self, df: pd.DataFrame, output_file: str):
        """Сохранение в Excel с форматированием"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Redress Kits', index=False)

                # Применяем форматирование
                workbook = writer.book
                worksheet = writer.sheets['Redress Kits']
                self._apply_formatting(worksheet, df)

        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            raise

    def _apply_formatting(self, worksheet, df: pd.DataFrame):
        """Применение форматирования к листу"""
        try:
            # Установка ширины колонок на основе конфига
            column_mapping = {
                col_idx: config
                for config in self.column_config['output_columns'].values()
                for col_idx, col_name in enumerate(df.columns, 1)
                if col_name == config['name']
            }

            for col_idx, config in column_mapping.items():
                col_letter = chr(64 + col_idx)  # A, B, C, ...
                worksheet.column_dimensions[col_letter].width = config['width']
                logger.debug(f"Set column {col_letter} width to {config['width']}")

            # Добавление фильтров
            if df.shape[0] > 0:  # Только если есть данные
                worksheet.auto_filter.ref = worksheet.dimensions

            # Форматирование заголовков
            header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
            for cell in worksheet[1]:
                cell.font = Font(bold=True, size=12)
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center')

            logger.debug("Formatting applied successfully")

        except Exception as e:
            logger.warning(f"Error applying formatting: {e}")
