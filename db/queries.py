from sqlalchemy.orm import Session, aliased
from sqlalchemy import select
from db.engine import engine
from db.tables import Bill, Committee, House


def get_all_full_rows():
    session = Session(engine)
    # Only return bill data as first step
    # TODO: join with all other relevant data
    committee_houses = aliased(House)
    bills = session.query(
        Bill.bill_number, Bill.name, Bill.full_text, Bill.author,
        Bill.status, Bill.session,
        House.name.label('origin_house_name'),
        Committee.name.label('committee_name'),
        Committee.webpage_link.label('committee_webpage'),
        committee_houses.name.label('committee_house')
    ).\
        join(House, Bill.origin_house_id==House.house_id).\
        join(Committee, Bill.committee_id==Committee.committee_id).\
        join(committee_houses, Committee.house_id==committee_houses.house_id)
    session.close()
    return bills
