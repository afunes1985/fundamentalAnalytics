from dao.fileDataDao import FileDataDao

fdList = FileDataDao.getFileDataYearPeriodList()

prevRow = None
periodList = ["FY", "Q1", "Q2", "Q3"]
foundPeriodList = []
for row in fdList:
    if (row[0] == 50863):
        if(prevRow is not None):
            if(prevRow[0] == row[0] and prevRow[1] == row[1]): # compare CIK and Year
                foundPeriodList.append(row[2])
            else:
                missingPeriodList = [x for x in periodList if x not in (y for y in foundPeriodList)]
                for missingPeriod in missingPeriodList:
                    print("PERIOD NOT FOUND: CIK: " + str(prevRow[0]) + " YEAR: " + str(prevRow[1]) + " PERIOD " + str(missingPeriod))
                foundPeriodList = []
                foundPeriodList.append(row[2])
        else:
            foundPeriodList.append(row[2])
        prevRow = row