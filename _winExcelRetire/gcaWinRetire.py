import pandas as pd
import getpass
import os
import time
from logger_setup import fridaylog
from dbConfig import con_to_db

prefix = r"C:\Users"
localuser = getpass.getuser()
out_path = prefix + "\\" + localuser + r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Automate Audit Win10L Returns"

conn = con_to_db("isolatedsafety")

data = pd.read_sql('EXEC GCAAssetMGMT_2_0.Asset.uspRetWinLapPast7Days', conn)
if not data.empty:
    print(data)
    TodaysDate = time.strftime("%Y%m%d")
    excelfilename = out_path + '\\' + TodaysDate + " - W10L Returns Past 7 Days.xlsx"

    file_exists = os.path.exists(excelfilename)
    if file_exists:
        print(excelfilename)
        pass
    else:
        print('No File Exists')
        options = {'strings_to_formulas': False, 'strings_to_urls': False}
        writer = pd.ExcelWriter(excelfilename, engine='xlsxwriter', options=options)

        data.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        worksheet.write_comment('A1',
                                '--Color Key--\nGREEN:\nRetired "Returned to Warehouse\n\nRED:\nDevice not found in Automate\n\nYELLOW:\nDuplicate device images found\n\n+++ORANGE:\n Specifies which duplicate Assignee/Image was retired',
                                {'x_scale': 3, 'y_scale': 3})
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'fg_color': '#00B0F0',
            'border': 1})
        for col_num, value in enumerate(data.columns.values):
            worksheet.write(0, col_num + 0, value, header_format)
        writer.save()
else:
    print("No GCA PC's to Retire.")
    fridaylog.info("No GCA PC's to Retire")