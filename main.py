import logging
from src.utils.config_loader import ConfigLoader
from src.utils.logger import setup_logging
from src.data.excel_reader import ExcelDataReader
from src.data.data_processor import DataProcessor
from src.core.reservation_engine import ReservationEngine, ReservationStrategy
from src.core.inventory_manager import InventoryManager
from src.core.report_generator import ReportGenerator


def main():
    # Настройка логирования
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Redress Kit Collector")

        # Загрузка конфигурации
        config_loader = ConfigLoader("config")
        settings = config_loader.load_settings()
        column_mapping = config_loader.load_column_mapping()

        # Инициализация компонентов
        excel_reader = ExcelDataReader(config_loader)
        processor = DataProcessor()

        reservation_strategy = ReservationStrategy(settings['reservation']['strategy'])
        reservation_engine = ReservationEngine(strategy=reservation_strategy)
        inventory_manager = InventoryManager(reservation_engine)

        report_generator = ReportGenerator(column_mapping)

        # Чтение данных
        logger.info("Reading input data...")
        required_redress_kits, required_redress = excel_reader.read_required_redress()
        redress_kit_bom, items = excel_reader.read_redress_bom(required_redress)
        stock_data = excel_reader.read_stock_data(items)

        # Обработка данных
        logger.info("Processing data...")
        required_with_items = processor.merge_consist(required_redress_kits, redress_kit_bom)
        processed_data, updated_stock = inventory_manager.merge_store_data(required_with_items, stock_data)
        # Генерация отчета
        logger.info("Generating report...")
        output_file = config_loader.get_output_file_path()
        report = report_generator.generate_redress_report(processed_data, output_file)

        logger.info("Redress System completed successfully")

    except Exception as e:
        logger.critical(f"Application failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
