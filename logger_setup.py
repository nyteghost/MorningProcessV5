import getpass
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import date

today = date.today()
strtoday = str(today)

prefix = r"C:\Users"
localuser = getpass.getuser()
log_folder = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\logs"
log_location = prefix + "\\" + localuser + log_folder + "\\"

# Logging Settings
formatter1 = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
formatter2 = logging.Formatter('%(levelname)s %(message)s')
logmode = 0


def setup_logger(name, log_file, logmode='', format='', level=logging.DEBUG):
    """
    To set up as many loggers as you want
    """

    handler = TimedRotatingFileHandler(log_file, when='D', interval=1, backupCount=90, encoding='utf-8', delay=False)
    if logmode == 1:
        handler = logging.FileHandler(log_file, mode='w')
    if format == 0:
        handler.setFormatter(formatter1)
    elif format == 1:
        handler.setFormatter(formatter2)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# logs
# importlog = setup_logger('imports' ,log_location+'.Imports.log')

fridaylog = setup_logger('ccmwinretirelog', log_location + "\\" + "Friday" + "\\" + 'CCMWinRetire.log')
collectionslog = setup_logger('collections',
                              log_location + "\\" + "Collections" + "\\" + 'Collections {}.log'.format(strtoday))
studentStafflog = setup_logger(f'student-staff_imports',
                               log_location + "\\" + "Student and Staff" + "\\" + '.StudentStaff {today}.log', 1)
upsclaimsscraperlog = setup_logger('upsclaimsscrape',
                                   log_location + "\\" + "UPS Claims" + "\\" + '.UPSClaimsScrape.log')
errlog = setup_logger('errors', log_location + 'Error.log', )

# refreshlog = setup_logger('refresh' , log_location+'.Process - Refresh.log')
# refresherrlog = setup_logger('refresherrors' , log_location+'Error Process - Refresh.log',1)
# fridaylog = setup_logger('friday' , log_location+'Friday Process.log')
# returnlog = setup_logger('Returns' , log_location+'Returns.log',format=1)
# addresslog = setup_logger('Addess' , log_location+'Address.log',format=1)








