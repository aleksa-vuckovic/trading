from datetime import datetime
from pathlib import Path
from unittest import TestCase
from compliance import nlb

goog_buy = Path(__file__).parent/"goog-buy.pdf"
goog_sell = Path(__file__).parent/"goog-sell.pdf"

client = nlb.Client(
    full_name = "ALEKSA VUČKOVIĆ",
    address = "DONJOVREŽINSKA 006/17",
    email = "aleksa.vuckovic36@gmail.com",
    bank="NLB Komercijalna Banka AD"
)
class TestNlb(TestCase):
    def test_parse_confirmation_goog_buy(self):
        confs = nlb.parse_confirmation(goog_buy)
        self.assertEqual(1, len(confs))
        conf = confs[0]
        
        expect = nlb.Confirmation(
            id = "K-2024/3939",
            client = client,
            order = nlb.Order(
                date = datetime(2024, 9, 25, tzinfo=None),
                security_type = "AKCIJE",
                issuer = "Alphabet Inc. - C Shares",
                amount = 3.0,
                currency = "USD",
                isin = "US02079K1079",
                symbol = "GOOG",
                exchange = "XNAS - NASDAQ"
            ),
            fills = [nlb.Fill(
                timestamp = datetime(2024, 9, 25, 21, 31, 20, tzinfo=None),
                amount=3.0,
                price=163.0,
                fees = 2.2
            )],
            total_fees = 24.23
        )

        self.assertEqual(expect.id, conf.id)
        self.assertEqual(expect.client, conf.client)
        self.assertEqual(expect.order, conf.order)
        self.assertEqual(expect.fills, conf.fills)
        self.assertEqual(expect, conf)

    def test_parse_confirmation_goog_sell(self):
        confs = nlb.parse_confirmation(goog_sell)
        self.assertEqual(1, len(confs))
        conf = confs[0]
        
        expect = nlb.Confirmation(
            id = "P-2024/3861",
            client = client,
            order = nlb.Order(
                date = datetime(2024, 12, 16, tzinfo=None),
                security_type = "AKCIJE",
                issuer = "Alphabet Inc. - C Shares",
                amount = 3,
                currency = "USD",
                isin = "US02079K1079",
                symbol = "GOOG",
                exchange = "XNAS - NASDAQ"
            ),
            fills = [nlb.Fill(
                timestamp = datetime(2024, 12, 16, 15, 30, 18, tzinfo=None),
                amount=3,
                price=194.4,
                fees=2.62
            )],
            total_fees=24.1
        )

        self.assertEqual(expect, conf)
