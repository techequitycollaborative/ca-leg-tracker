from marshmallow import Schema, fields, post_load
from db.tables import Bill


class AugmentedBillSchema(Schema):
    """
    Augmented Bill Marshmallow Schema

    Marshmallow schema used for loading/dumping Bills augmented with House and Committee Data.
    """
    bill_number = fields.String(allow_none=False)
    name = fields.String()
    full_text = fields.String()
    author = fields.String()
    origin_house_name = fields.String()
    committee_name = fields.String()
    committee_webpage = fields.String()
    committee_house = fields.String()
    status = fields.String()
    session = fields.String()

    @post_load
    def make_bill(self, data, **kwargs):
        return Bill(**data)

