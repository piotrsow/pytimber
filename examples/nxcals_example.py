import pytimber

nxcals=pytimber.NXCals()


data=nxcals.DevicePropertyQuery.system('CMW')\
        .startTime('2018-05-23 00:05:54.500').endTime('2018-05-23 00:06:54.500')\
        .entity().device('ZT10.QFO03').property('Acquisition').buildDataset()\
        .select('cyclestamp', 'current').orderBy('cyclestamp', ascending=False).collect()

for row in data:
    print(list(row.values()))


