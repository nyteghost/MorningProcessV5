from re import sub

def filereplace(filename,regexToReplace,replacementString):
    a = open(filename, 'r+')
    file_content = a.read()
    a.seek(0)
    a.truncate(0)
    a.write(sub(regexToReplace,replacementString,file_content))
    a.close()