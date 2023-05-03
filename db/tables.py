from typing import List
from typing import Optional
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Bill(Base):
    __tablename__ = "bill"
    __table_args__ = {'schema': 'ca'}

    bill_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    bill_number: Mapped[str]
    full_text: Mapped[Optional[str]]
    author: Mapped[Optional[str]]
    origin_house_id: Mapped[Optional[int]]
    committee_id: Mapped[Optional[int]]
    status: Mapped[Optional[str]]
    session: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"Bill(id={self.bill_id!r}, bill_number={self.bill_number!r}, name={self.name!r})"
