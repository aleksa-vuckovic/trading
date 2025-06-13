from datetime import datetime
from pathlib import Path
from unittest import TestCase
from compliance import nlb

goog = Path(__file__).parent/"goog.pdf"
class TestNlb(TestCase):
    def test_parse_confirmation_goog(self):
        confs = nlb.parse_confirmation(goog)
        self.assertEqual(1, len(confs))
        conf = confs[0]
        
        expect = nlb.Confirmation(
            id = "P-2024/3861",
            client = nlb.Client(
                full_name = "ALEKSA VUČKOVIĆ",
                address = "DONJOVREŽINSKA 006/17",
                email = "aleksa.vuckovic36@gmail.com",
                bank="NLB Komercijalna Banka AD"
            ),
            order = nlb.Order(
                date = datetime(2024, 12, 16),
                security_type = "AKCIJE",
                issuer = "Alphabet Inc. - C Shares",
                amount = 3,
                currency = "USD",
                isin = "US02079K1079",
                symbol = "GOOG",
                exchange = "XNAS - NASDAQ"
            ),
            transaction = nlb.Transaction(
                timestamp = datetime(2025, 12, 16, 15, 30, 18),
                amount=3,
                price=194.4,
                total_price=583.2,
                bank_fee = 1.75,
                execution_fee = 0.87,
                other_fee = 0
            )
        )

        self.assertEqual(expect, conf)


