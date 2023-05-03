from datetime import date
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
    origin_house_id: Mapped[Optional[int]] = mapped_column(ForeignKey("house.id"))
    committee_id: Mapped[Optional[int]] = mapped_column(ForeignKey("committee.id"))
    status: Mapped[Optional[str]]
    session: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"Bill(id={self.bill_id!r}, bill_number={self.bill_number!r}, name={self.name!r})"


class BillHistory(Base):
    __tablename__ = "bill_history"
    __table_args__ = {'schema': 'ca'}

    bill_history_id: Mapped[int] = mapped_column(primary_key=True)
    bill_id: Mapped[int] = mapped_column(ForeignKey("bill.id"))
    entry_date: Mapped[Optional[date]]
    entry_text: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"BillHistory(id={self.bill_history_id!r}, bill_id={self.bill_id!r}, entry_date={self.entry_date!r}, entry_text={self.entry_text!r})"


class Committee(Base):
    __tablename__ = "committee"
    __table_args__ = {'schema': 'ca'}

    committee_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    webpage_link: Mapped[Optional[str]]
    house_id: Mapped[Optional[int]] = mapped_column(ForeignKey("house.id"))

    def __repr__(self) -> str:
        return f"Committee(id={self.committee_id!r}, name={self.name!r})"


class CommitteeVoteResult(Base):
    __tablename__ = "committee_vote_result"
    __table_args__ = {'schema': 'ca'}

    committee_vote_result_id: Mapped[int] = mapped_column(primary_key=True)
    vote_date: Mapped[Optional[date]]
    bill_id: Mapped[int] = mapped_column(ForeignKey("bill.id"))
    committee_id: Mapped[int] = mapped_column(ForeignKey("committee.id"))
    votes_for: Mapped[Optional[int]]
    votes_against: Mapped[Optional[int]]

    def __repr__(self) -> str:
        return f"CommitteeVoteResult(id={self.committee_vote_result_id!r}, bill_id={self.bill_id!r}, votes_for={self.votes_for!r}, votes_against={self.votes_against!r})"


class House(Base):
    __tablename__ = "house"
    __table_args__ = {'schema': 'ca'}

    house_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    def __repr__(self) -> str:
        return f"House(id={self.house_id!r}, name={self.name!r})"


class HouseVoteResult(Base):
    __tablename__ = "house_vote_result"
    __table_args__ = {'schema': 'ca'}

    house_vote_result_id: Mapped[int] = mapped_column(primary_key=True)
    vote_date: Mapped[Optional[date]]
    bill_id: Mapped[int] = mapped_column(ForeignKey("bill.id"))
    house_id: Mapped[int] = mapped_column(ForeignKey("house.id"))
    votes_for: Mapped[Optional[int]]
    votes_against: Mapped[Optional[int]]

    def __repr__(self) -> str:
        return f"CommitteeVoteResult(id={self.house_vote_result_id!r}, bill_id={self.bill_id!r}, votes_for={self.votes_for!r}, votes_against={self.votes_against!r})"

