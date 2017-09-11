# Main file
import nielsen_manager_lib
import schedule
import time

# Initialization
nielsen_manager_lib.init()
jConf = nielsen_manager_lib.loadConfig()

# Load parameters
reloadFlag = jConf['mainConf'][0]['reload']
rPath = jConf['initFolders'][0]['rPath']


# Functions
def afterSendData():
    for folder in jConf['dataFolders']:
        if folder['enabled'] == 'true':
            nielsen_manager_lib.getDataFromFolders(folder, rPath)
    nielsen_manager_lib.backUp(rPath)
    nielsen_manager_lib.removeNonPrintableChr(rPath)
    nielsen_manager_lib.fixShopNumber(rPath)
    nielsen_manager_lib.weeklyReport(rPath, jConf['mainConf'])


def beforeSentData():
    nielsen_manager_lib.weeklyCleaning(rPath)

# def reLoadConfig():
#    if reloadFlag == 'true':
#       jConf = nielsen_manager_lib.loadConfig()


# Scheduling tasks
schedule.every().tuesday.at("11:50").do(afterSendData)
schedule.every().tuesday.at("13:30").do(beforeSentData)
schedule.every().wednesday.at("12:30").do(afterSendData)
schedule.every().wednesday.at("13:00").do(beforeSentData)
schedule.every().sunday.at("13:00").do(beforeSentData)
# schedule.every(10).seconds.do(reLoadConfig)
# schedule.every(240).seconds.do(moveFilesToDataFolder)
# schedule.every(10).seconds.do(countfiles)


# Starting tasks  !!!!sposta prima il one shot!!!
if jConf['mainConf'][0]['scheduler'] == 'true':
    print 'Scheduler mode is active! The tasks are starting...'
    while 1:
        schedule.run_pending()
        time.sleep(0.5)
else:
    print 'Scheduler is not active. No tasks will be started'
    # One shot
    if jConf['mainConf'][0]['oneshot'] == 'true':
        print 'One shot mode is active! The tasks are starting...'
        afterSendData()
        beforeSentData()
    else:
        'One shot mode is not active No tasks will be started'
