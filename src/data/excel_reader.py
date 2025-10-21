import pandas as pd
from typing import Tuple, Dict, List
import logging
from src.utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ExcelDataReader:
    def __init__(self, config_loader: ConfigLoader):
        self.config_loader = config_loader
        self.sheet_names = config_loader.get_sheet_names()

    def read_required_redress(self) -> Tuple[Dict, pd.Series]:
        """Загрузка требуемых китов"""
        file_path = self.config_loader.get_input_file_path()
        sheet_name = self.sheet_names['required']

        try:
            redress = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            required_redress_kits = {"Required redress kit": []}

            column_config = self.config_loader.load_settings()['columns']['input']['required']
            redress_kit_col = column_config['redress_kit']
            qty_on_store_col = column_config['qty_on_store']
            required_qty_col = column_config['required_qty']

            required_redress = redress[redress_kit_col].dropna().str.upper()

            for redress_kit, req_qty in redress.groupby(redress_kit_col, sort=False):
                kit_data = {
                    "redress_kit": redress_kit.upper(),
                    "total": [
                        {
                            "q-ty on store": row[qty_on_store_col],
                            "required": row[required_qty_col]
                        }
                        for _, row in req_qty.iterrows()
                    ]
                }
                required_redress_kits["Required redress kit"].append(kit_data)

            logger.info(f"Data from {sheet_name} uploaded successfully")
            return required_redress_kits, required_redress

        except Exception as e:
            logger.error(f"Error reading required redress data: {e}")
            raise

    def read_redress_bom(self, required_redress: pd.Series) -> Tuple[Dict, List]:
        """Загрузка BOM данных"""
        file_path = self.config_loader.get_input_file_path()
        sheet_name = self.sheet_names['bom']

        try:
            rk_bom = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            redress_kit_bom = {"redress kit consist": []}

            column_config = self.config_loader.load_settings()['columns']['input']['bom']
            redress_part_col = column_config['redress_part_number']
            item_part_col = column_config['item_part_number']
            description_col = column_config['description']
            quantity_col = column_config['quantity']

            # Фильтрация по требуемым наборам
            if required_redress is not None and not required_redress.empty:
                f_rk_bom = rk_bom[rk_bom[redress_part_col].str.upper().isin(required_redress)]
            else:
                f_rk_bom = rk_bom

            items = f_rk_bom[item_part_col].dropna().str.upper()

            for redress_kit_, redress_kit_items in f_rk_bom.groupby(redress_part_col):
                kit_data = {
                    "redress kit": redress_kit_.upper(),
                    "consist": [
                        {
                            'item': row[item_part_col],
                            'description': row[description_col],
                            'qty': row[quantity_col]
                        }
                        for _, row in redress_kit_items.iterrows()
                        if pd.notna(row[quantity_col]) and row[quantity_col] not in (0, '0')
                    ]
                }
                redress_kit_bom["redress kit consist"].append(kit_data)

            logger.info(f"Data from {sheet_name} uploaded successfully")
            return redress_kit_bom, items.tolist()

        except Exception as e:
            logger.error(f"Error reading BOM data: {e}")
            raise

    def read_stock_data(self, items: List[str]) -> pd.DataFrame:
        """Загрузка данных склада"""
        file_path = self.config_loader.get_input_file_path()
        sheet_name = self.sheet_names['stock']

        try:
            column_config = self.config_loader.load_settings()['columns']['input']['stock']
            part_number_col = column_config['part_number']
            serial_number_col = column_config['serial_number']
            total_col = column_config['total']

            stock_dataframe = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

            # Если items пустой, возвращаем все данные
            actual_stock = stock_dataframe[stock_dataframe['Part Number'].isin(items)]
            missing_in_stock = [item for item in items if item not in actual_stock['Part Number'].values]
            missing_items_df = pd.DataFrame({'Part Number': missing_in_stock})

            # добавление в DataFrame отсутствующих частей
            actual_stock_ = pd.concat([
                actual_stock,
                missing_items_df
            ], ignore_index=True).fillna({'SN': '', 'Total': 0})

            # Проверяем наличие дубликатов
            duplicates = actual_stock_[actual_stock_.duplicated(subset=['Part Number', 'SN'], keep=False)]

            # Удаляем дубликаты, оставляя первую запись
            actual_stock_clean = actual_stock_.drop_duplicates(subset=['Part Number', 'SN'], keep='first')

            #print(f"После удаления дубликатов: {len(actual_stock_clean)} строк")

            return actual_stock_clean

        except Exception as e:
            logger.error(f"Error reading stock data: {e}")
            raise
