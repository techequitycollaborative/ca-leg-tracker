from marshmallow import Schema, fields, post_load
from db.tables import Bill


class BillSchema(Schema):
    """
    Bill Marshmallow Schema

    Marshmallow schema used for loading/dumping Bills
    """
    bill_id = fields.Integer(allow_none=False)
    name = fields.String()
    bill_number = fields.String()
    full_text = fields.String()
    author = fields.String()
    origin_house_id = fields.Integer()
    committee_id = fields.Integer()
    status = fields.String()
    session = fields.String()

    @post_load
    def make_bill(self, data, **kwargs):
        return Bill(**data)

