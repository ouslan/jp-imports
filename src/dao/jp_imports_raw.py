from sqlmodel import Field, Session, SQLModel, select
from typing import Optional

class JPTradeData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    trade: str
    year: int
    month: int
    hs: str
    commodity_short_name: str
    commodity_description: str
    cty_code: int
    country: str
    subcountry_code: str
    district: str
    district_desc: str
    district_posh: str
    district_posh_desc: str
    data: int
    sitc: str
    sitc_short_desc: str
    sitc_long_desc: str
    naics: str
    naics_description: str
    end_use_i: str
    end_use_e: str
    hts_desc: str
    unit_1: str
    qty_1: int
    unit_2: str
    qty_2: int
    ves_val_mo: int
    ves_wgt_mo: int
    cards_mo: int
    air_val_mo: int
    air_wgt_mo: int
    dut_val_mo: int
    cal_dut_mo: int
    con_cha_mo: int
    con_cif_mo: int
    gen_val_mo: int
    gen_cha_mo: int
    gen_cif_mo: int
    air_cha_mo: int
    ves_cha_mo: int
    cnt_cha_mo: int
    rev_data: str


def create_jp_trade_data_table(engine):
    SQLModel.metadata.create_all(engine)

def select_all_jp_trade_data(engine):
    with Session(engine) as session:
        statement = select(JPTradeData)
        return session.exec(statement).all()
