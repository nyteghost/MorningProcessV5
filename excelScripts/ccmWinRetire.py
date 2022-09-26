import pandas as pd
import getpass
import os
import time
from mpConfigs.logger_setup import fridaylog
from mpConfigs.dbConfig import dbConnect

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets"
out_path2 = prefix + "\\" + localuser + r"\Southeastern Computer Associates, LLC\SCA Warehouse - SCA Warehouse Operations\Cloud CM Solutions\CCM Returns"

# logger = setup_logger('first_logger', 'Friday Process.log' ,0)


conn = dbConnect("isolatedsafety")

data2 = pd.read_sql('EXEC uspRetWinLapPast7Days', conn)
if not data2.empty:
    print(data2)

    TodaysDate = time.strftime("%Y%m%d")
    excelfilename2 = out_path2 + '\\' + TodaysDate + " - CCM Returns Last 7 Days.xlsx"
    file_exists2 = os.path.exists(excelfilename2)
    if file_exists2:
        print(file_exists2)
        pass
    else:
        options = {'strings_to_formulas': False, 'strings_to_urls': False}
        writer2 = pd.ExcelWriter(excelfilename2, engine='xlsxwriter', options=options)

        data2.to_excel(writer2, index=False)
        workbook2 = writer2.book
        worksheet2 = writer2.sheets['Sheet1']

        worksheet2.write_comment('A1',
                                 '--Color Key--\nGREEN:\nRetired "Returned to Warehouse\n\nRED:\nDevice not found in Automate\n\nYELLOW:\nDuplicate device images found\n\n+++ORANGE:\n Specifies which duplicate Assignee/Image was retired',
                                 {'x_scale': 3, 'y_scale': 3})
        header_format = workbook2.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'fg_color': '#00B0F0',
            'border': 1})
        for col_num, value in enumerate(data2.columns.values):
            worksheet2.write(0, col_num + 0, value, header_format)
        writer2.save()
else:
    print("No CCM PC's to retire.")
    fridaylog.info("No CCM PC's to retire.")
