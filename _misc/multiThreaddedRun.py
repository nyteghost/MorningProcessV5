from concurrent.futures import ThreadPoolExecutor


def studentImport():
    import _imports.nuStudent


def staffImport():
    import _imports.nuStaff


def collectionsImport():
    import _imports.nuCollections


def apiChromeOS():
    import _imports.nuChromeOS


def wsUPSOutbound():
    import _webscrapes.upsOutbound


def wsUPSClaims():
    import _webscrapes.upsClaims


def wsUPSCapitalClaims():
    import _webscrapes.upsCapitalClaims


def importUPSOutbound():
    import _imports.upsOutbound


def importUPSClaims():
    import _imports.upsClaims


def sortAsapRemoval():
    import _connectWise.asapRemoval


def sortBarcodeImgRename():
    import _sorting.barcodeRename


def sortAddressVerify():
    import _imports.addressValidation


def sortTracking2JPG():
    import _sorting.tracking2jpg


def allGCAPeople():
    with ThreadPoolExecutor() as executor:
        executor.submit(studentImport)
        executor.submit(staffImport)
        executor.submit(collectionsImport)


def allScrape():
    with ThreadPoolExecutor() as executor:
        # executor.submit(apiChromeOS)
        executor.submit(wsUPSOutbound)
        executor.submit(wsUPSClaims)
        executor.submit(wsUPSCapitalClaims)


def allImport():
    with ThreadPoolExecutor() as executor:
        executor.submit(importUPSOutbound)
        executor.submit(importUPSClaims)
        executor.submit(wsUPSCapitalClaims)


def allSort():
    with ThreadPoolExecutor() as executor:
        executor.submit(sortAsapRemoval)
        executor.submit(sortBarcodeImgRename)
        executor.submit(sortAddressVerify)
        executor.submit(sortTracking2JPG)


allScrape()




# driver = webdriver.Chrome()
# str1 = driver.capabilities['browserVersion']
# str2 = driver.capabilities['chrome']['chromedriverVersion'].split(' ')[0]
# print(str1)
# print(str2)
# print(str1[0:3])
# print(str2[0:3])
# if str1[0:3] != str2[0:3]:
#   print("please download correct chromedriver version")
