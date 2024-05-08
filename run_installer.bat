pyinstaller index_pushOutageData.py --onefile
pyinstaller index_pushWbesSdlData.py --onefile
pyinstaller index_pushIntraStateSdlData.py --onefile
pyinstaller index_pushScadaActualData.py --onefile

xcopy /y dist\index_pushOutageData.exe index_pushOutageData.exe*
xcopy /y dist\index_pushWbesSdlData.exe index_pushWbesSdlData.exe*
xcopy /y dist\index_pushIntraStateSdlData.exe index_pushIntraStateSdlData.exe*
xcopy /y dist\index_pushScadaActualData.exe index_pushScadaActualData.exe*
