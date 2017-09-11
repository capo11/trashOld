import datetime
from email.mime.text import MIMEText
import ftplib
# import glob
import json
import logging
import os
import re
import shutil
import smtplib


def init():
    try:
        if not os.path.isfile('./nielsen_manager.conf'):
            print "ERROR: nielsen_manager.conf NOT FOUND. The configuration file will be generated with default values\n"
            confData = {}

            confData['dataFolders'] = []
            confData['dataFolders'].append({
                'name': 'path_1',
                'address': 'tabacchigest.com',
                'abs_path': '/ftp/nielsen/data',
                'usr': '',
                'pwd': '',
                'enabled': 'false'
            })
            confData['dataFolders'].append({
                'name': 'path_3',
                'address': 'dev1.tgsgroup.it',
                'abs_path': '',
                'usr': 'nielsen_server',
                'pwd': '',
                'enabled': 'false'
            })

            confData['initFolders'] = []
            confData['initFolders'].append({
                'name': 'rootPath',
                'rPath': '/ftp/nielsen',
                'active': 'true'
            })
            confData['initFolders'].append({
                'name': 'log',
                'active': 'false'
            })
            confData['initFolders'].append({
                'name': 'storage',
                'active': 'false'
            })
            confData['initFolders'].append({
                'name': 'bkp',
                'active': 'false'
            })
            confData['initFolders'].append({
                'name': 'blacklist',
                'active': 'false'
            })
            confData['initFolders'].append({
                'name': 'data',
                'active': 'false'
            })

            confData['mainConf'] = []
            confData['mainConf'].append({
                'reload': 'false',
                'scheduler': 'false',
                'oneshot': 'false'
            })
            with open('./nielsen_manager.conf', 'w') as confFile:
                json.dump(confData, confFile, sort_keys=True,
                          indent=4, ensure_ascii=False)
                # log('./',20,"Creato file di inpostazioni ")
                print "nielsen_manager.conf was generated\n"
            confFile.close()
    except Exception as e:
        print 'Errore nella creazione del file di configurazione nielsen_manager.conf'
        raise e
    # legge il file di conf e controlla se esistono le cartelle nel path del conf
    try:
        jConf = loadConfig()
        # root
        # if not os.path.exists(jConf['initFolders'][0]['rPath']):
        #   os.makedirs(jConf['initFolders'][0]['rPath'])
        #   log('./',20,"")
        # log
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'log')):
            os.makedirs(os.path.join(jConf['initFolders'][0]['rPath'], 'log'))
            log('./', 20, "Creata cartella LOG")
        # storage
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'storage')):
            os.makedirs(os.path.join(
                jConf['initFolders'][0]['rPath'], 'storage'))
            log('./', 20, "Creata cartella STORAGE")
        # bkp
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'bkp')):
            os.makedirs(os.path.join(jConf['initFolders'][0]['rPath'], 'bkp'))
            log('./', 20, "Creata cartella BKP")
        # blacklist
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'blacklist')):
            os.makedirs(os.path.join(
                jConf['initFolders'][0]['rPath'], 'blacklist'))
            log('./', 20, "Creata cartella BLACKLIST")
        # data
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'data')):
            os.makedirs(os.path.join(jConf['initFolders'][0]['rPath'], 'data'))
            log('./', 20, "Creata cartella DATA")
        if not os.path.exists(os.path.join(jConf['initFolders'][0]['rPath'], 'trash')):
            os.makedirs(os.path.join(jConf['initFolders'][0]['rPath'], 'trash'))
            log('./', 20, "Creata cartella TRASH")
    except Exception as e:
        print 'errore nella verifica e creazione delle cartelle'
        raise e


def loadConfig():
    try:
        with open('./nielsen_manager.conf') as confFile:
            config = json.load(confFile)
            confFile.close()
            return config
    except Exception as e:
        print e


def getDataFromFolders(folder, rPath):
    log(rPath, 40, "inizio procedura di recupero files per la cartella : " +
        folder['address'] + folder['abs_path'])
    dPath = os.path.join(rPath, 'data')  # ./data/
    sPath = folder['abs_path']
    fileName = getFileName()
    if folder['usr'] != '' and folder['pwd'] != '':
        try:
            ftp = ftplib.FTP(folder['address'])
            ftp.login(folder['usr'], folder['pwd'])
            filematch = fileName + '*'
            # Loop - looking for matching files
            for filename in ftp.nlst(filematch):
                fhandle = open(os.path.join(dPath, filename), 'wb')
                ftp.retrbinary('RETR ' + filename, fhandle.write)
                # da sostituire con move to trash folder
                ftp.delete(filename)
                fhandle.close()
        except Exception as e:
            print e
            log(rPath, 40, "impossibile collegarsi al server ftp" +
                folder['address'])
            raise
        finally:
            ftp.close()
    else:
        try:
            files = os.listdir(sPath)
            for f in files:
                if fileName in f:
                    shutil.move(os.path.join(sPath, f), dPath)
        except Exception as e:
            print e


def backUp(rPath):
    log(rPath, 20, "inizio procedura di backup")
    sPath = os.path.join(rPath, 'data')
    dPath = os.path.join(os.path.join(rPath, 'bkp'), getLastWeek())  # ./bkp/22
    fileName = getFileName()
    try:
        if not os.path.exists(dPath):
            os.makedirs(dPath)
        files = os.listdir(sPath)
        for f in files:
            if fileName in f:
                shutil.copy2(os.path.join(sPath, f), dPath)
    except Exception as e:
        print e
        log(rPath, 40, "impossibile eseguire uno o piu backup")
        raise


def weeklyCleaning(rPath):
    log(rPath, 20, "inizio procedura di pulizia settimanale")
    week = getLastWeek()
    sPath = os.path.join(rPath, 'data')
    dPath = os.path.join(rPath, 'storage/' + week)
    fileName = "NEWTGS_"  # getFileName()
    try:
        if not os.path.exists(dPath):
            os.makedirs(dPath)
        files = os.listdir(sPath)
        for f in files:
            if fileName in f:
                shutil.move(os.path.join(sPath, f), dPath)
    except Exception as e:
        print e


def removeNonPrintableChr(rPath):
    log(rPath, 20, "inizio procedura di rimozione caratteri non printabili")
    fileName = getFileName()
    try:
        sPath = os.path.join(rPath, 'data/')
        files = os.listdir(sPath)
        for f in files:
            if fileName in f:
                with open(os.path.join(sPath, f), "r") as dirty:
                    text = dirty.read()
                    clean = open(os.path.join(sPath, f), "w")
                    clean.write(re.sub(r'[\x1F]', ' ', text))
                    dirty.close()
                    clean.close()
    except Exception as e:
        print e


def arrivedFilesCounter(rPath, mode=1):
    fileName = getFileName()
    if mode == 1:
        sPath = os.path.join(rPath, 'data')
        arrived = len([name for name in os.listdir(sPath) if fileName in name])
        return arrived
    elif mode == 2:
        pass

# Restituisce il numero della settimana precedente a quella attuale


def getLastWeek():
    # crate object now
    now = datetime.datetime.now()
    # get actual week's number from object now
    week = now.isocalendar()[1]
    if week == 1:
        week = 52
    else:
        week -= 1
    return str(week)

# Restituisce l'anno della settimana precedente


def getYear():
    now = datetime.datetime.now()
    year = now.year
    # mettere controllo su week 52  e week 1
    if getLastWeek() == 52:
        year -= 1
    return str(year)

# Costruisce il nome del file di nielsen della settimana
# precedente alla corrente


def getFileName():
    week = getLastWeek()
    pad = '0'
    if week < 10:
        pad = '00'
    return "NEWTGS_" + getYear() + pad + week + '_001'


'''
Level                   value
CRITICAL                50
ERROR                   40
WARNING                 30
INFO                    20
DEBUG                   10
NOTSET                  0
'''


def log(rPath='./', type=10, msg='Generical log - this is a default value'):
    # Create logger
    logfile = os.path.join(rPath, 'log/nielsen_manager.log')
    # print logfile
    logger = logging.getLogger('nielsen_manager')
    hdlr = logging.FileHandler(logfile)
    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    logger.log(type, msg)


def messageOnScreen(type=10, msg='default message'):
    pass


def confInterface(rpath):
    pass

# List of status
# 0 Arrived
# 1 Missing
# 2 blacklist
# 3 anomalous
# 4 error_duplicate


def blackAndCheck(rPath, fix=0):
    if fix == 0:
        fixShopNumber(rPath)
    fileName = getFileName()
    shopList = [{'arrived': 0, 'missing': 0, 'blacklist': 0, 'anomalous': 0, 'error': 0}]
    try:
        sPath = os.path.join(rPath, 'data')
# Check over file +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        with open(os.path.join(sPath, "shop.lst"), "r") as shopList:
            for shop in shopList:
                shopNum, shopPiva, active = shop.split(":")
                fullPath = os.path.join(sPath, (fileName + shopNum.zfill(7) + "[" + shopPiva + "[" + ".txt"))
                print fullPath
                if os.path.isfile(fullPath):
                    # File found
                    print "ok --"
                    if active == 1:
                        # File arrived and shop is active, all ok!
                        if shopList[shopNum] is None:
                            shopList[shopNum]({'path': fullPath, 'status': 0})
                            shopList[0]['arrived'] += 1
                        else:
                            shopList.append({'shop': shopNum, 'path': fullPath, 'status': 4})
                            shopList[0]['error'] += 1
                    else:
                        # File arrived but shop isn't active
                        if shopList[shopNum] is None:
                            shopList[shopNum]({'path': fullPath, 'status': 2})
                            shopList[0]['blacklist'] += 1
                        else:
                            shopList.append({'shop': shopNum, 'path': fullPath, 'status': 4})
                            shopList[0]['error'] += 1
                            # blacklist.extend((shopNum ,fullPath))
                else:
                    print "ko"
                    # File not Found
                    if shopList[shopNum] is None:
                        shopList[shopNum]({'path': 'NULL', 'status': 1})
                        shopList[0]['missing'] += 1
                    else:
                        shopList.append({'shop': shopNum, 'path': fullPath, 'status': 4})
                        shopList[0]['error'] += 1
# Check over directory+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        files = os.listdir(sPath)
        for file in files:
            shopNum = file[22:-17]
            if shopList[shopNum] is None:
                shopList[shopNum]({'path': file, 'status': 3})
                shopList[0]['anomalous'] += 1
            else:
                shopList.append({'shop': shopNum, 'path': fullPath, 'status': 4})
                shopList[0]['error'] += 1
    except Exception as e:
        print e
# Generate report and move to blacklist
    writeWeeklyReport(shopList)
    shopList.pop(0)
    for shop in shopList:
        if shop['status'] == 2:
            moveToBlackList(rPath, shop['path'])
        # testa con arrivati, non arrivati, anomali, errori
        # apro il file shop e da quello ci creo una lista degli shop con {week; pive; shop; attivo; arrivato = no; anomalo =0 }
        # leggo i file su disco; per ogni file presente compilo la lista verificando l'associazione shop-piva e aggiungendo i file
        # che non mi aspetto esserci anomali
        # sposto i non attivi arrivati in black list
        # scrivo file di report con gli attivi arrivati (solo tot); gli attivi non arrivati; i non attivi arrivati (black list); gli anomali (verificare)


def moveToBlackList(rPath, sPath):
    dPath = os.path.join(rPath, 'blacklist')
    try:
        if not os.path.exists(dPath):
            os.makedirs(dPath)
            log(rPath, 30, 'The black list folder does not exsist, it will be created now!')
        if os.path.isfile(sPath):
            shutil.move(sPath, dPath)
            log(rPath, 30, "The file " + sPath + 'is moved to black list')
        else:
            log(rPath, 40, 'I tried to move the file but no longer exists!!!')
    except Exception as e:
        raise e


def sendWeeklyReport(rPath, sbj, frm, to, usr, pwd, srv, prt):
    numberOfFiles = arrivedFilesCounter(rPath)
    now = datetime.datetime.now()
    report = "Weekly report \n Data arrived: " + \
        now.strftime('%d/%m/%Y') + \
        "\n Number of files arrived in this week: " + numberOfFiles
    msg = MIMEText(report)
    msg['Subject'] = sbj
    msg['From'] = frm
    msg['To'] = to
    server = smtplib.SMTP(srv + ':' + prt)
    server.ehlo()
    server.starttls()
    server.login(usr, pwd)
    server.sendmail(frm, to, msg.as_string())
    server.quit()


def writeWeeklyReport(shopList):
    text = '\t+------------------------------+\t|     Weekly report  w%u       |\t+------------------------------+\n' % (getLastWeek())
    text.append('Number of arrived files: %u\n' % (shopList[0]['arrived']))
    text.append('Number of missing files: %u\n' % (shopList[0]['missing']))
    text.append('Number of files in blacklist: %u\n' % (shopList[0]['blacklist']))
    text.append('Number of errors: %u\n' % (shopList[0]['error']))
    print text


def fixShopNumber(rPath):
    sPath = os.path.join(rPath, 'data/')
    try:
        files = os.listdir(sPath)  # all files in the folder /data
        for file in files:
            shopChk = file[22:-17]
            pivaChk = file[26:-5]
            with open(os.path.join(rPath, 'nielsen_manager.fix')) as fixFile:
                for fix in fixFile:
                    shopOk, pivaOk = fix.split(":")
                    if (pivaChk == pivaOk) and (shopChk != shopOk):
                        with open(os.path.join(sPath, file), "r+") as fToFix:
                            line = fToFix.readline()
                            fixedLine = line[0:25] + shopOk
                            fToFix.seek(0, 0)
                            fToFix.write(fixedLine)
                            os.rename(os.path.join(sPath, file), os.path.join(
                                sPath, file[:22] + shopOk + file[25:]))
    except Exception as e:
        print e


def trash(rPath):
    listaDir = os.listdir(os.path.join(rPath, 'data'))
    for linea in listaDir:
        linea.strip()
        if "OLD" in linea:
            shutil.move(os.path.join(rPath, 'data'), './trash')

    pass
