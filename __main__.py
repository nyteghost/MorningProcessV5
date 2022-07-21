from fileinput import filename
from unittest import skip
# import logging
from excelScripts.refreshAll import refresh, refreshModifiedcheck
from mpConfigs.util_lib import apple, applejack, banana, cherry, cleanUp, check
from mpConfigs.logger_setup import studentStafflog, collectionslog
import datetime
import time
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table
import smtplib, ssl
import os, sys
import getpass
import subprocess
from loguru import logger
import better_exceptions
from _msGraph.teamSend import TeamsChat
from mpConfigs.doorKey import config

better_exceptions.hook()
better_exceptions.MAX_LENGTH = None

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)


t = datetime.datetime.now()
log_date = t.strftime('%Y-%m-%d')

startTime = time.time()

mpReport = TeamsChat('mpReport')

# For scripts
log_config = {
    "handlers": [
        {"sink": sys.stdout, "format": "{time} - {message}"},
        {"sink": "./logs/Daily-{time:M-D-YYYY}.log", "serialize": True},
    ],
    "extra": {"user": "someone"}
}
logger.configure(**log_config)
logger.add("./logs/Process.log", backtrace=True, diagnose=True, rotation='monday')
logger.info('Start of Today\'s log.')


def time_in_range(day_start, end, current):
    """Returns whether current is in the range [start, end]"""
    return day_start <= current <= end


day_start = datetime.time(0, 0, 0)
middle = datetime.time(11, 30, 0)
end = datetime.time(23, 55, 0)
current = datetime.datetime.now().time()

completion_list = []
success_list = []
failed_list = []
failed_reason = []
failedImports = []
# skipped_list=[]

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


### Set days of week
now = datetime.datetime.now()
day = now.weekday()
dt = datetime.datetime.now()
date_processed = "{}-{}-{}_{}-{}-{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

# log_location = r"C:\Users" + "\\"+ getpass.getuser() + r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\logs\Complete" + "\\"

# Logging setup
# logging.basicConfig(filename=log_location+f'Completion{date_processed}.log', encoding='utf-8',filemode='w')

excelDriveFiles = []


def preMorning():
    ### Student Import
    try:
        student_import = 0
        if applejack() == "Exists":
            logger.info("Importing nuStudent Test Sheet")
            import _imports.nuStudent
            logger.info('nuStudent Import Complete')
            success_list.append('nuStudent Import')
            failed_list.append('')
            student_import = 1
        else:
            excelDriveFiles.append('No nuStudent Sheet found.')
            logger.info("No nuStudent Test Sheet today.")
            studentStafflog.info('A Updated nuStudent Test Sheet was not found.')
            failedImports.append("Student")
    except Exception as ex:
        studentStafflog.info(str(ex))
        print(ex)
        studentStafflog.error('Issue with nuStudent Test Sheet')
        logger.info('nuStudent Test Sheet Import Failed')
        success_list.append('')
        failed_list.append('nuStudent Test Sheet Import')
        failed_reason.append("nuStudent Test Sheet Import: " + str(ex))
        pass

    ### Staff Import
    try:
        staff_import = 0
        if banana() == "Exists":
            logger.info("Importing Staff Sheet")
            import _imports.nuStaff
            logger.info('Staff Import Complete')
            success_list.append('Staff Import')
            failed_list.append('')
            staff_import = 1
        else:
            excelDriveFiles.append('No Staff Sheet found.')
            logger.info("No Staff Sheet today.")
            studentStafflog.info('A Updated Staff Sheet was not found.')
            failedImports.append("Staff")
    except Exception as ex:
        studentStafflog.error(ex)
        studentStafflog.error('Issue with Staff Sheet')
        logger.info('Staff Import Failed')
        success_list.append('')
        failed_list.append('Staff Import')
        failed_reason.append("Staff Import: " + str(e))
        pass

    ### Collections Import
    try:
        collections_import = 0
        if cherry() == "Exists":
            logger.info("Importing Collections Sheet")
            import _imports.nuCollections
            logger.info('Collections Import Complete')
            success_list.append('Collections Import')
            failed_list.append('')
            collections_import = 1
        else:
            excelDriveFiles.append('No Collections Sheet found.')
            logger.info("No Collections Sheet today.")
            collectionslog.info('A Updated Collections Sheet was not found.')
            failedImports.append("Collections")
    except Exception as ex:
        collectionslog.error(ex)
        collectionslog.error('Issue with Collections Sheet')
        logger.info('Collections Import Failed')
        success_list.append('')
        failed_list.append('Collections Import')
        failed_reason.append("Collections Import: " + str(e))
        pass

    if failedImports:
        reportError = TeamsChat('mpReport')
        for drive in "Z":
            drive += ':'

            try:
                os.scandir(drive)
                status = "accessible"
            except Exception:
                status = "non accessible"

            print(drive, os.access(drive, os.R_OK), status)
            if status == 'non accessible':
                # reportError.send('This is a test of the reporting system.')
                reportError.send(f'Python was unable to discover the {drive} drive. ', 'Josh')
            else:
                # reportError.send('This is a test of the reporting system.')
                reportError.send(f'Missing {failedImports} from {drive} Drive.', 'Josh')
            reportError.send(f'Please hold off until the imports are complete!', 'Elijah')

    return student_import, staff_import, collections_import


def main_imports():
    ### Scraping Data ###
    try:
        import _imports.nuChromeOS
    except Exception as e:
        logger.error(f'chromeos_device_info failed due to {e}')
        success_list.append('')
        failed_list.append(f'ChromeOS Import')
        failed_reason.append("ChromeOS Import: " + str(e))
    else:
        logger.info(f'chromeos_device_info success.')
        success_list.append('ChromeOS Import')
        failed_list.append('')

    try:
        ups_outbound_pass = 0
        import _webscrapes.upsOutbound  ## Outbound
    except Exception as e:
        logger.error(f'_data_scrapes.ups_outbound failed due to {e}')
        success_list.append('')
        failed_list.append('UPS Outbound Scrape')
        failed_reason.append("Outbound Scrape: " + str(e))
    else:
        logger.info(f'_data_scrapes.ups_outbound success.')
        success_list.append(f'UPS Outbound Scrape')
        failed_list.append('')
        ups_outbound_pass = 1

    try:
        ups_claims_pass = 0
        import _webscrapes.upsClaims  ## Outbound
    except Exception as e:
        logger.error(f'_data_scrapes.ups_claims failed due to {e}')
        success_list.append('')
        failed_list.append(f'UPS Claims Scrape')
        failed_reason.append("UPS Claims: " + str(e))
    else:
        logger.info(f'_data_scrapes.ups_claims success.')
        success_list.append(f'UPS Claims Scrape')
        failed_list.append('')
        ups_claims_pass = 1

    try:
        ups_capital_claims_pass = 0
        import _webscrapes.upsCapitalClaims ## claim_submission_summary
    except Exception as e:
        logger.error(f'_data_scrapes.ups_capital_claims  failed due to {e}')
        success_list.append('')
        failed_list.append(f'UPS Capital Claims Scrape')
        failed_reason.append("UPS Capital Claims: " + str(e))
    else:
        logger.info(f'_data_scrapes.ups_capital_claims success.')
        success_list.append(f'UPS Capital Claims Scrape')
        failed_list.append('')
        ups_capital_claims_pass = 1

    ### Imports ###
    if ups_outbound_pass == 1:
        try:
            import _imports.upsOutbound
        except Exception as e:
            logger.error(f'_database_imports.ups_outbound  failed due to {e}')
            success_list.append('')
            failed_list.append(f'UPS Outbound Database Import')
            failed_reason.append("UPS Outbound Database Import: " + str(e))
        else:
            logger.info(f'_database_imports.ups_outbound success.')
            success_list.append(f'UPS Outbound Database Import')
            failed_list.append('')
    else:
        success_list.append('')
        failed_list.append(f'UPS Outbound Database Import')
        failed_reason.append(f'Outbouund failed= {ups_outbound_pass}')

    if ups_claims_pass == 1 and ups_capital_claims_pass == 1:
        try:
            import _imports.upsClaims
        except Exception as e:
            logger.error(f'_database_imports.ups_claims failed due to {e}')
            success_list.append('')
            failed_list.append(f'UPS Claims Database Import')
            failed_reason.append("UPS Outbound Database Import: " + str(e))
        else:
            logger.info(f'_database_imports.ups_claims success.')
            success_list.append(f'UPS Claims Database Import')
            failed_list.append('')
    else:
        success_list.append('')
        failed_list.append(f'UPS Claims Database Import')
        failed_reason.append(f'UPS claims pass={ups_claims_pass},ups_capital_claims_pass={ups_capital_claims_pass} ')

    ### Sort the ASAP photos into folders by month, day, and FID
    try:
        import _sorting.asapPhotoSort
    except Exception as e:
        logger.error(f'_database_imports.asap_photo_sort failed due to {e}')
        success_list.append('')
        failed_list.append(f'ASAP Photo Sort')
        failed_reason.append("ASAP Photo Sort: " + str(e))
    else:
        logger.info(f'_database_imports.asap_photo_sort success.')
        success_list.append(f'ASAP Photo Sort')
        failed_list.append('')

    ### ASAP ticket completion removal
    try:
        import _connectWise.asapRemoval
    except Exception as e:
        logger.error(f'_misc.asap_removal failed due to {e}')
        success_list.append('')
        failed_list.append(f'ASAP Removal')
        failed_reason.append("ASAP removal: " + str(e))
    else:
        logger.info(f'_database_imports.asap_photo_sort success.')
        success_list.append(f'ASAP Removal')
        failed_list.append('')

    ### Address Validations
    try:
        import _imports.addressValidation
    except Exception as e:
        logger.error(f'_api_data.ups_address_validation failed due to {e}')
        success_list.append('')
        failed_list.append(f'UPS/USPS Address Verification')
        failed_reason.append("Address  Validation: " + str(e))
    else:
        logger.info(f'_api_data.ups_address_validation success.')
        success_list.append(f'UPS/USPS Address Verification')
        failed_list.append('')

    ### Returns/Address Verification
    try:
        import _connectWise.retAddCheck
    except Exception as e:
        logger.error(f'_misc.return_addresschange_reopened failed due to {e}')
        success_list.append('')
        failed_list.append(f'Return/Address Change/Re-Opened')
        failed_reason.append("Address/return email: " + str(e))
    else:
        logger.info(f'_misc.return_addresschange_reopened success.')
        success_list.append(f'Return/Address Change/Re-Opened')
        failed_list.append('')

    ### Automate Reboot
    try:
        from _connectWise.rebootAutomate import runIt
    except Exception as e:
        logger.error(f'_misc.rebootAutomate failed due to {e}')
        success_list.append('')
        failed_list.append(f'CW Automate Reboot')
        failed_reason.append("CW Automate Reboot: " + str(e))
    else:
        logger.info(f'_misc.rebootAutomate success.')
        success_list.append(f'CW Automate Reboot')
        failed_list.append('')

def refresh_process(student_import, staff_import, collection_import):
    ### RefreshALl
    try:
        refresh(filename='inventory')
    except Exception as e:
        logger.error(f'There was a problem with refreshing GCA Inventory File.;\n{e}')
        success_list.append('')
        failed_list.append(f' GCA Inventory  RefreshAll')
        failed_reason.append("UPS Outbound Database Import: " + str(e))
    else:
        logger.info(f'Refreshing GCA Inventory success.')
        success_list.append(f'GCA Inventory  RefreshAll')
        failed_list.append('')

    if student_import == 1:
        try:
            refresh(filename='student')
        except Exception as e:
            logger.error(f'There was a problem with refreshing Student File;\n{e}')
            success_list.append('')
            failed_list.append(f'Student RefreshAll')
            failed_reason.append("Student Refreshall: " + str(e))
        else:
            logger.info(f'Refreshing Student success.')
            success_list.append(f'Student RefreshAll')
            failed_list.append('')
    else:
        # skipped_list.append('Student RefreshAll')
        failed_list.append(f'')

    if staff_import == 1:
        try:
            refresh(filename='staff')
        except Exception as e:
            logger.error(f'here was a problem with refreshing the Staff File;\n{e}')
            success_list.append('')
            failed_list.append(f'Staff RefreshAll')
            failed_reason.append("Staff RefreshAll: " + str(e))
        else:
            logger.info(f'Refreshing Staff success.')
            success_list.append(f'Staff RefreshAll')
            failed_list.append('')
    else:
        # skipped_list.append('Staff RefreshAll')
        failed_list.append(f'')

    if collection_import == 1:
        try:
            refresh(filename='collections')
        except Exception as e:
            logger.error(f'There was a problem with refreshing the Collections File;\n{e}')
            success_list.append('')
            failed_list.append(f'Collections RefreshAll')
            failed_reason.append("Collectiosn RefreshAll: " + str(e))
        else:
            logger.info(f'Refreshing Collections success.')
            success_list.append(f'Collections RefreshAll')
            failed_list.append('')
    else:
        # skipped_list.append('Collections RefreshAll')
        failed_list.append(f'')

    try:
        refresh(filename='asap')
    except Exception as e:
        logger.error(f'There was a problem with refreshing the ASAP Post File;\n{e}')
        success_list.append('')
        failed_list.append(f'ASAP RefreshAll')
        failed_reason.append("ASAP RefreshAll: " + str(e))
    else:
        logger.info(f'Refreshing ASAP Post success.')
        success_list.append(f'ASAP RefreshAll')
        failed_list.append('')

    try:
        refresh(filename='ccm')
    except Exception as e:
        logger.error(f'There was a problem with refreshing the CCM Inventory File;\n{e}')
        success_list.append('')
        failed_list.append(f'CCM Inventory RefreshAll')
        failed_reason.append("CCM Inventory RefreshAll: " + str(e))
    else:
        logger.info(f'Refreshing CCM Inventory success.')
        success_list.append(f'CCM Inventory RefreshAll')
        failed_list.append('')

    try:
        refresh(filename='cbenroll')
    except Exception as e:
        logger.error(f'There was a problem with refreshing the CB Enroll Issues File;\n{e}')
        success_list.append('')
        failed_list.append(f'CB Enroll Issues RefreshAll')
        failed_reason.append("CB Enroll Issues RefreshAll: " + str(e))
    else:
        logger.info(f'Refreshing CB Enroll Issues success.')
        success_list.append(f'CB Enroll Issues RefreshAll')
        failed_list.append('')

    ### Verify All refreshed
    try:
        refreshModifiedcheck()
    except Exception as e:
        logger.error(f'lastModified function failed due to {e}')
        success_list.append('')
        failed_list.append(f'Last Modified Refresh')
        failed_reason.append("Last Modified Refresh: " + str(e))
    else:
        logger.info(f'lastModified success.')
        success_list.append(f'Last Modified Refresh')
        failed_list.append('')


def temp_imports():
    # try:
    #     import temporary.student_graduate_equipment_response_2022
    # except Exception as e:
    #     logger.error(f'2022 Graduate Equipment Return Form failed due to {e}')
    #     success_list.append('')
    #     failed_list.append(f'2022  Graduate Equipment Return Form')
    #     failed_reason.append("2022 Graduate Equipment Return Form: "+str(e))
    # else:
    #     logger.info(f'2022 Graduate Equipment Return Form success.')
    #     success_list.append('2022 Graduate Equipment Return Form')
    #     failed_list.append('')

    # try:
    #     import temporary.student_equipment_response_2022
    # except Exception as e:
    #     logger.error(f'2022 Equipment Return Form failed due to {e}')
    #     success_list.append('')
    #     failed_list.append(f'2022 Equipment Return Form')
    #     failed_reason.append("2022  Equipment Return Form: "+str(e))
    # else:
    #     logger.info(f'2022 Equipment Return Form success.')
    #     success_list.append('2022 Equipment Return Form')
    #     failed_list.append('')

    # try:
    #     import temporary.summerStudent
    # except Exception as e:
    #     logger.error(f'2022 summerStudent failed due to {e}')
    #     success_list.append('')
    #     failed_list.append(f'2022 summerStudent')
    #     failed_reason.append("2022 summerStudent: "+str(e))
    # else:
    #     logger.info(f'2022 summerStudent success.')
    #     success_list.append('2022 summerStudent')
    #     failed_list.append('')
    pass


@logger.catch
def main():
    ###### MAIN RUN ######
    cleanUp()
    if day != 6:
        if day != 0 and dt.time() < datetime.time(8, 30):
            try:
                import _msGraph.forcePassReset
            except Exception as e:
                logger.error(f'ms_account_force_pass_reset failed due to {e}')
                success_list.append('')
                failed_list.append(f'ms_account_force_pass_reset')
                failed_reason.append("Force Password Reset: " + str(e))
            else:
                logger.info(f'ms_account_force_pass_reset success.')
                success_list.append(f'ms_account_force_pass_reset')
                failed_list.append('')

        elif day == 0 and dt.time() < datetime.time(8, 30):
            try:
                import _msGraph.accountBlockUser
            except Exception as e:
                logger.error(f'ms_account_block_user failed due to {e}')
                success_list.append('')
                failed_list.append(f'ms_account_block_user')
                failed_reason.append("MS Account Block: " + str(e))
            else:
                logger.info(f'ms_account_block_user success.')
                success_list.append(f'ms_account_block_user')
                failed_list.append('')

        if dt.time() < datetime.time(11, 0):
            student_import, staff_import, collect_import = preMorning()
            main_imports()
            # temp_imports()
            refresh_process(student_import, staff_import, collect_import)
        elif datetime.time(11, 0) < dt.time() < datetime.time(15, 0):
            main_imports()
            try:
                import _imports.nuTmobReport
            except Exception as e:
                logger.error(f'There was a problem with T-Mobile Subscriber Report Import.\n{e}')
                success_list.append('')
                failed_list.append(f'T-Mobile Subscriber Report Import')
                failed_reason.append("T-Mobile sub report import: " + str(e))
            else:
                logger.info(f'T-Mobile Subscriber Report Import success.')
                success_list.append(f'T-Mobile Subscriber Report Import')
                failed_list.append('')
            refresh_process(0, 0, 0)
        elif dt.time() > datetime.time(17, 30):
            refresh_process(0, 0, 0)

        if day == 4:
            if time_in_range(day_start, middle, current):
                try:
                    import excelScripts.ccmWinRetire
                except Exception as e:
                    logger.error(f'ccmWinRetire failed due to {e}')
                    success_list.append('')
                    failed_list.append(f'ccmWinRetire')
                    failed_reason.append("ccmWinRetire: " + str(e))
                else:
                    logger.info(f'ccmWinRetire success.')
                    success_list.append(f'ccmWinRetire')
                    failed_list.append('')

                try:
                    import excelScripts.gcaWinRetire
                except Exception as e:
                    logger.error(f'gcaWinRetire failed due to {e}')
                    success_list.append('')
                    failed_list.append(f'gcaWinRetire')
                    failed_reason.append("gcaWinRetire: " + str(e))
                else:
                    logger.info(f'gcaWinRetire success.')
                    success_list.append(f'gcaWinRetire')
                    failed_list.append('')

            if time_in_range(middle, end, current):
                try:
                    import _connectWise.fBar
                except Exception as e:
                    logger.error(f'fBar failed due to {e}')
                    success_list.append('')
                    failed_list.append(f'fBar report automation')
                    failed_reason.append("FBar ticket generator: " + str(e))
                else:
                    logger.info(f'gcaWinRetire success.')
                    success_list.append(f'fBar report automation')
                    failed_list.append('')

        # print('This is Success_List:')
        # print(success_list)
        # print("This is Failed_List")
        # print(failed_list)


def emailResults():
    completion_df = pd.DataFrame(list(zip(success_list, failed_list)), columns=['Passed', 'Failed'])
    print(completion_df)
    completeTime = (time.time() - startTime)
    time_to_complete = int(completeTime / 60), 'minutes', int(completeTime) % 60, 'seconds to complete import.'
    print(time_to_complete)

    time_now = localtime = time.asctime(time.localtime(time.time()))
    sender_email = "mbrownscaadmin@georgiacyber.org"
    coworkers = ["mbrown@sca-atl.com", "nbowman@sca-atl.com", "Aadeli@sca-atl.com", "emorris@sca-atl.com", ]
    # coworkers= ["mbrown@sca-atl.com"]
    password = (config['google']['mailapp'])
    message = MIMEMultipart("alternative")
    message["Subject"] = "Import Process Completion Information" + str(time_now)
    message["From"] = sender_email
    message['To'] = ', '.join(coworkers)

    html = """\
    <html>
    <head></head>
    <body>
        Time to complete: {}.
        {}
        <p style="font-size:30px"><b>Import Process Completion</b></p>
        {}
        <br>
        Failed Scripts;
        <br>
        {}
    </body>
    </html>
    """.format(time_to_complete, excelDriveFiles,
               build_table(completion_df, 'blue_light', font_size='14px', width='auth'), [x for x in failed_reason])

    part1 = MIMEText(html, 'html')
    message.attach(part1)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, coworkers, message.as_string())
        server.quit()


def toTeams():
    completion_df = pd.DataFrame(list(zip(success_list, failed_list)), columns=['Passed', 'Failed'])
    print('sendTable')
    mpReport.sendTable("MP Completion", 5, completion_df)
    if failed_reason:
        mpReport.send("Failure Reason</br>"+"</br>".join(failed_reason))


def asap_image_rename_email(status):
    time_now = localtime = time.asctime(time.localtime(time.time()))
    sender_email = "mbrownscaadmin@georgiacyber.org"
    coworkers = ["mbrown@sca-atl.com", "nbowman@sca-atl.com", "emorris@sca-atl.com", "Aadeli@sca-atl.com", ]
    # coworkers= ["mbrown@sca-atl.com"]
    password = (config['google']['mailapp'])
    message = MIMEMultipart("alternative")
    message["Subject"] = "Asap Image Rename" + str(time_now)
    message["From"] = sender_email
    message['To'] = ', '.join(coworkers)

    html = """\
    <html>
    <head></head>
    <body>
        <p style="font-size:30px"><b>Asap image rename based on barcodes {}.</b></p>
        <br>
    </body>
    </html>
    """.format(status)

    part1 = MIMEText(html, 'html')
    message.attach(part1)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, coworkers, message.as_string())
        server.quit()


try:
    main()
except Exception as e:
    today = datetime.date.today()
    Date = today
    logger.add(f"./logs/crash-{Date}.log")
    logger.add(sys.stderr, format="{time} {level} {message}")
    logger.info(e)
    toTeams()
    mpReport.send(f"Morning Process crashed. See {e}")
    exit(1)
else:
    toTeams()

time.sleep(30)
asap_img_folder = now.strftime("%Y-%m-%d")
dirName = r'C:\Users\SCA\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Pickup Data\{}'.format(
    asap_img_folder)  # here your dir path

if dt.time() > datetime.time(17, 30):
    isdir = os.path.isdir(dirName)
    if isdir:
        try:
            subprocess.call(r"_misc\barcode_img_rename.py", shell=True)
            import _sorting.tracking2jpg
        except Exception as e:
            asap_image_rename_email(f'failed due to {e}')
        else:
            asap_image_rename_email('complete')
