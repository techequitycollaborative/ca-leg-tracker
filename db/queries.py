from sqlalchemy.orm import Session
from sqlalchemy import select
from db.engine import engine
from db.tables import Bill


def get_all_full_rows():
    session = Session(engine)
    # Only return bill data as first step
    # TODO: join with all other relevant data
    bills = session.query(Bill).all()
    return bills
