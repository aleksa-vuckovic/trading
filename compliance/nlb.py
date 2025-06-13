from dataclasses import dataclass
from pathlib import Path
import re
import pdfplumber
from datetime import datetime

from base import dates

@dataclass
class Client:
    full_name: str
    address: str
    email: str
    bank: str

@dataclass
class Order:
    date: datetime
    security_type: str
    issuer: str
    amount: float
    currency: str
    isin: str
    symbol: str
    exchange: str

@dataclass
class Transaction:
    timestamp: datetime
    amount: float
    price: float
    total_price: float
    bank_fee: float
    execution_fee: float
    other_fee: float

@dataclass
class Confirmation:
    id: str
    client: Client
    order: Order
    transaction: Transaction

file = "D:\\brokeri\\Potvrde_o_realizovanim_ino_nalozima - 2024-09-26T135155.478 - GOOG.pdf"

_confirmation_id_pattern = re.compile(r"(?i)nalog broj:\s*[A-Z]-\d{4}/\d+")
def _nn[T](ref: T|None) -> T:
    assert ref
    return ref
def parse_confirmation(file: Path) -> list[Confirmation]:
    result: list[Confirmation] = []
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            id_match = _confirmation_id_pattern.findall(text)
            assert id_match, f"Can't find id. Text: {text}"
            assert len(id_match) == 1, f"Multiple id matches. Text {text}"
            id = id_match[0]

            tables = list(page.extract_tables())
            table = tables[0]
            client = Client(
                full_name = _nn(table[1][1]),
                address = _nn(table[3][1]),
                email = _nn(table[5][1]),
                bank = _nn(table[3][3])
            )

            table = tables[1]
            order = Order(
                date = dates.str_to_datetime(_nn(table[1][1]), format="%d.%m.%Y"),
                security_type = _nn(table[1][4]),
                issuer = _nn(table[2][4]),
                amount = float(_nn(table[3][1])),
                currency = _nn(table[4][1]),
                isin = _nn(table[3][4]),
                symbol = _nn(table[4][4]),
                exchange = _nn(table[5][1])
            )

            table = tables[2]
            transaction = Transaction(
                timestamp = dates.str_to_datetime(re.sub(r"\s+", " ", _nn(table[1][0])), format="%d.%m.%Y %H:%M:%S"),
                amount = float(_nn(table[1][1])),
                price = float(_nn(table[1][2])),
                total_price = float(_nn(table[1][3])),
                bank_fee = float(_nn(table[1][4])),
                execution_fee = float(_nn(table[1][5])),
                other_fee = float(_nn(table[1][6]))
            )

            result.append(Confirmation(id, client, order, transaction))
        
        return result
