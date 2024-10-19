from src.jp_imports.data_process import DataTrade

def test_jp_columns_yt():
    assert DataTrade().process_int_jp("yearly", "total").columns == ['year', 'imports', 'exports', 'qty_imports', 'qty_exports']

def test_jp_columns_yn():
    assert DataTrade().process_int_jp("yearly", "naics").columns == ['year','naics_id','naics_code','imports','exports','qty_imports','qty_exports']