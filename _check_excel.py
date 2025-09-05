import pandas as pd
path = r".\data\faq.xlsx"
xls = pd.ExcelFile(path)
print("Sheets:", xls.sheet_names)
for name in xls.sheet_names:
    try:
        df = pd.read_excel(path, sheet_name=name, nrows=3)
        print(f"[{name}] cols={list(df.columns)}")
    except Exception as e:
        print(f"[{name}] READ ERROR: {e}")
