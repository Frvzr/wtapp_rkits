from pydantic import BaseModel, field_validator
from typing import List, Dict, Optional, Union
from enum import Enum


class ReservationStrategy(str, Enum):
    LOWEST_SN_FIRST = "lowest_sn_first"
    FIFO = "fifo"
    HIGHEST_QTY_FIRST = "highest_qty_first"


class Component(BaseModel):
    item: str
    description: str
    qty: float

    @field_validator('item')
    @classmethod
    def uppercase_item(cls, v: str) -> str:
        return v.upper()


class StockItem(BaseModel):
    part_number: str
    serial_number: str
    total: float

    @field_validator('part_number')
    @classmethod
    def uppercase_part_number(cls, v: str) -> str:
        return v.upper()

    @field_validator('serial_number', mode='before')
    @classmethod
    def convert_serial_to_string(cls, v: any) -> str:
        """Конвертирует serial_number в строку, если это число"""
        if v is None:
            return ""
        if isinstance(v, (int, float)):
            # Для целых чисел убираем .0, для дробных оставляем как есть
            if isinstance(v, float) and v.is_integer():
                return str(int(v))
            return str(v)
        return str(v).strip()


class KitTotal(BaseModel):
    qty_on_store: float
    required: float


class RedressKit(BaseModel):
    redress_kit: str
    total: List[KitTotal]
    consist: List[Component]
    max_collect_items: List[Dict]
    qty_on_store: List[Dict]
    reserved: List[Dict]
    serial: List[Dict]


class ReservationResult(BaseModel):
    reserved: List[Dict]
    update_data: List[StockItem]


class AppConfig(BaseModel):
    input_file: str
    output_file: str
    reservation_strategy: ReservationStrategy
    allow_partial_reservation: bool
    low_stock_threshold: int
