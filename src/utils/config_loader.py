import yaml
from typing import Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    def __init__(self, config_dir: str = "config"):
        # Получаем абсолютный путь к config директории
        current_dir = Path(__file__).parent.parent.parent
        self.config_dir = current_dir / config_dir
        self._settings = None
        self._column_mapping = None

    def load_settings(self) -> Dict[str, Any]:
        """Загрузка основных настроек"""
        if self._settings is None:
            settings_path = self.config_dir / "settings.yaml"
            self._settings = self._load_yaml(settings_path)
            logger.info("Settings loaded successfully")
        return self._settings

    def load_column_mapping(self) -> Dict[str, Any]:
        """Загрузка маппинга колонок"""
        if self._column_mapping is None:
            mapping_path = self.config_dir / "column_mapping.yaml"
            self._column_mapping = self._load_yaml(mapping_path)
            logger.info("Column mapping loaded successfully")
        return self._column_mapping

    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """Загрузка YAML файла"""
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading config file {file_path}: {e}")
            raise

    def get_input_file_path(self) -> str:
        """Получение пути к входному файлу"""
        settings = self.load_settings()
        input_file = settings['files']['input_file']

        # Если путь относительный, делаем его абсолютным относительно корня проекта
        if not Path(input_file).is_absolute():
            project_root = self.config_dir.parent
            return str(project_root / input_file)
        return input_file

    def get_output_file_path(self) -> str:
        """Получение пути к выходному файлу"""
        settings = self.load_settings()
        output_file = settings['files']['output_file']

        # Если путь относительный, делаем его абсолютным относительно корня проекта
        if not Path(output_file).is_absolute():
            project_root = self.config_dir.parent
            return str(project_root / output_file)
        return output_file

    def get_reservation_strategy(self) -> str:
        """Получение стратегии резервирования"""
        settings = self.load_settings()
        return settings['reservation']['strategy']

    def get_sheet_names(self) -> Dict[str, str]:
        """Получение названий листов"""
        settings = self.load_settings()
        return {
            'required': settings['sheets']['input']['required'],
            'bom': settings['sheets']['input']['bom'],
            'stock': settings['sheets']['input']['stock'],
            'main': settings['sheets']['output']['main']
        }

    def get_column_names(self, data_type: str) -> Dict[str, str]:
        """Получение названий колонок для типа данных"""
        settings = self.load_settings()
        return settings['columns']['input'][data_type]
