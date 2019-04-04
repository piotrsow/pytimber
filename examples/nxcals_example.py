import pytimber

db=pytimber.NXCals()

lst=db.searchVariable("LHC%BCT%INT%")

t1='2018-05-23 00:05:54.500'
t2='2018-05-23 00:06:54.500'

df= db.getVariable('LHC.BCTFR.A6R4.B1:BEAM_INTENSITY',t1,t2)



#basic query
dataset=db.DevicePropertyQuery.system('CMW')\
        .startTime('2018-05-23 00:05:54.500').endTime('2018-05-23 00:06:54.500')\
        .entity().device('ZT10.QFO03').property('Acquisition').buildDataset()

dataset.printSchema()

data=dataset.select('cyclestamp', 'current').orderBy('cyclestamp', ascending=False).collect()

for row in data:
    print(list(row.values()))

# from manual
import numpy

ref_time = numpy.datetime64('2018-05-23T00:05:54.500')
interval_start = ref_time  - numpy.timedelta64(1,'D')

ref_time=str(ref_time).replace('T',' ')
interval_start=str(ref_time).replace('T',' ')


df=db.DevicePropertyQuery.system('CMW')\
    .startTime(interval_start).endTime(ref_time)\
    .entity().device('ZT10.QFO03').property('Acquisition').buildDataset()\
    .select('cyclestamp', 'current').orderBy('cyclestamp', ascending=False).limit(1)

df.show()


# basic query
df=db.VariableQuery.system("CMW").variable("CPS.TGM:CYCLE")\
        .startTime("2018-06-15 23:00:00.000").endTime("2018-06-16 00:00:00.000")\
        .buildDataset()


# basic query
df = db.VariableQuery\
    .system('CMW').startTime('2018-04-29 00:00:00.000').endTime('2018-04-30 00:00:00.000')\
    .variableLike('%I_MEAS%')\
    .buildDataset()

data=df.collect()
list(data[0].values())

db._cern.nxcals.service.client.api.VariableService.findBySystemNameAndVariableNameLike("CMW","%I_MEAS%")
