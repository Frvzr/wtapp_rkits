from typing import Dict
import logging

logger = logging.getLogger(__name__)


class DataProcessor:

    @staticmethod
    def merge_consist(required_redress_kits: Dict, redress_kit_bom: Dict) -> Dict:
        """Объединение данных о требуемых китах и BOM"""
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

    @staticmethod
    def calculate_max_collect(qty_on_store: float, qty_per_kit: float) -> int:
        """Расчет максимального количества наборов"""
        from math import floor
        if qty_per_kit == 0:
            return 0
        return floor(qty_on_store / qty_per_kit)
