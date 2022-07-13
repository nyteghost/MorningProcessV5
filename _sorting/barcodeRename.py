import cv2
from pyzbar.pyzbar import decode, ZBarSymbol
import os
import re
import getpass
import time
from multiprocessing import Pool, Value, Lock
import multiprocessing
from alive_progress import alive_bar
import sys
from datetime import datetime

today = datetime.now()
asap_img_folder = today.strftime("%Y-%m-%d")

prefix = r"C:\Users"
localuser = getpass.getuser()
spfolderpath = r"Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Pickup Data"
folderdate = r"2022-03-08"
familyid = r"1546921 - Copy"
filefolder = prefix + '\\' + localuser + '\\' + spfolderpath + '\\' + folderdate + '\\' + familyid
dirName = r'C:\Users\SCA\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Pickup Data\{}'.format(
    asap_img_folder)  # here your dir path

### LIST ###
FileExistsErrorlst = []
FileNotFoundlst = []
OSErrorlst = []


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def writer_process(q):
    with open("datagathered", 'a') as out:
        out.write(q)
        out.write('\n')
        out.close()  # can't guess whether you really want this


def init(L):
    global lock
    lock = L


def getListOfFiles(dirName):
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
    return allFiles


# Make one method to decode the barcode


def BarcodeReader(image):
    # read the image in numpy array using cv2
    img = cv2.imread(image)
    img = cv2.resize(img, None, fx=12, fy=12, interpolation=cv2.INTER_LINEAR)

    # Decode the barcode image
    detectedBarcodes = decode(img, symbols=[ZBarSymbol.CODE39, ZBarSymbol.CODE128])

    # If not detected then print the message
    if not detectedBarcodes:
        # print("Barcode Not Detected or your barcode is blank/corrupted!")
        return 'BarcodeNotDetected'

    else:
        # Traverse through all the detected barcodes in image
        for barcode in detectedBarcodes:
            # Locate the barcode position in image
            (x, y, w, h) = barcode.rect

            # Put the rectangle in image using
            # cv2 to heighlight the barcode
            cv2.rectangle(img, (x - 10, y - 10),
                          (x + w + 10, y + h + 10),
                          (255, 0, 0), 2)

            # Print the barcode data
            if barcode.data != "":
                print(barcode.data)
                print(barcode.type)
                return barcode.data


def getFileNames():
    # Get the list of all files in directory tree at given path
    listOfFiles = getListOfFiles(dirName)
    listOfFiles = [x for x in listOfFiles if "Old Process" not in x]
    listOfFiles = [x for x in listOfFiles if "GCA-" not in x]
    listOfFiles = [x for x in listOfFiles if "SN-" not in x]
    listOfFiles = [x for x in listOfFiles if "BarcodeNotDetected" not in x]
    listOfFiles = [x for x in listOfFiles if "x1.png" not in x]
    listOfFiles = [x for x in listOfFiles if "x1x2.png" not in x]
    listOfFiles = [x for x in listOfFiles if x.endswith(".png")]
    # Print the files
    for elem in listOfFiles:
        if "Old Process" in elem or 'GCA-' in elem:
            input("Press ENTER to continue")
        else:
            print(elem)
    print("****************")

    # Get the list of all files in directory tree at given path
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
    listOfFiles = [x for x in listOfFiles if "Old Process" not in x]
    listOfFiles = [x for x in listOfFiles if "GCA-" not in x]
    listOfFiles = [x for x in listOfFiles if "SN-" not in x]
    listOfFiles = [x for x in listOfFiles if "BarcodeNotDetected" not in x]
    listOfFiles = [x for x in listOfFiles if "x1" not in x]
    listOfFiles = [x for x in listOfFiles if "x1x2" not in x]
    listOfFiles = [x for x in listOfFiles if x.endswith(".png")]
    print(len(listOfFiles))
    return listOfFiles
    # Print the files


def main(fileList):
    if fileList.endswith(".png"):
        newName = BarcodeReader(fileList)
        if 'BarcodeNotDetected' in str(newName):
            writer_process(fileList)
            print("Barcode Not Detected: ", fileList)
            pass
        elif 'GCA-' in str(newName):
            pass
        elif 'SN-' in str(newName):
            pass
        else:
            newName = str(newName)
            newName = newName.replace('b', '')
            newName = newName.replace("'", '')
            try:
                print(newName)
                if newName.isdigit():
                    os.rename(fileList, os.path.dirname(fileList) + '\\' + "GCA-" + newName + '.png')
                    newName = re.sub('[^0-9 ^a-z ^A-Z \n\-]', '', newName)
                    print("Rename: ", os.path.dirname(fileList) + '\\' + "GCA-" + newName + '.png' + '\n')
                else:
                    os.rename(fileList, os.path.dirname(fileList) + '\\' + "SN-" + newName + '.png')
                    print("Rename: ", os.path.dirname(fileList) + '\\' + "SN-" + newName + '.png' + '\n')
            except FileExistsError:
                try:
                    os.remove(fileList)
                except Exception as e:
                    print(e)
                    pass

    else:
        # print('Skipped ',fileList,' because the file is not a png.')
        pass


def main_asap_barcode_run():
    num_proc = multiprocessing.cpu_count()  # use all processors
    num_proc = 6
    startTime = time.time()
    try:
        fileList = getFileNames()
        fileListLen = len(fileList)
        pool = multiprocessing.Pool(2, initializer=init, initargs=(Lock(),))
        y = pool.map(main, fileList)
    except Exception as e:
        print(e)


#   print("No Bar Codes:")
#   print("\nFileExistError List")
#   print(FileExistsErrorlst)
#   print("\nFileNotFoundL ist")
#   print(FileNotFoundlst)
#   print("\nOSError List")
#   print(OSErrorlst)

main_asap_barcode_run()