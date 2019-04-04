#kinit -f -r 5d -kt ~/.keytab rdemaria
import cmmnbuild_dep_manager

mgr=cmmnbuild_dep_manager.Manager()

jpype=mgr.start_jpype_jvm()

org=jpype.JPackage('org')
cern=jpype.JPackage('cern')
java=jpype.java
System=java.lang.System

KeyValuesQuery=cern.nxcals.data.access.builders.KeyValuesQuery
DevicePropertyQuery=cern.nxcals.data.access.builders.DevicePropertyQuery

#System.setProperty("logging.config", "classpath:log4j2.yml")
#System.setProperty("springboot.appargs", "--debug")
System.setProperty("spring.main.web-application-type", "none")
System.setProperty("javax.net.ssl.trustStore", USER_HOME + "/.nxcals/nxcals_cacerts")
System.setProperty("javax.net.ssl.trustStorePassword", "nxcals")
USER_HOME = System.getProperty("user.home")
System.setProperty("kerberos.keytab", USER_HOME + "/.nxcals/keytab")
USER = System.getProperty("user.name")
System.setProperty("kerberos.principal", USER)
#System.setProperty("service.url", "http://cs-ccr-testbed2.cern.ch:19093,http://cs-ccr-testbed3.cern.ch:19093,http://cs-ccr-nxcalstbs1.cern.ch:19093")
System.setProperty("service.url", "http://cs-ccr-nxcals6.cern.ch:19093")
spark=cern.lhc.nxcals.util.NxcalsSparkSession.sparkSession()

#KeyValuesQuery.builder(spark).system("MOCK-SYSTEM").atTime("2018-05-09 17:30:46.300000000").entity().keyValue("device", "device2").buildDataset()

#d1=DevicePropertyQuery.builder(spark).system("CMW")\
#                           .startTime("2018-04-02 00:00:00.000")\
#                           .endTime("2018-05-02 00:00:00.000")\
#                           .fields(["USER","DEST"])\
#                           .entity().parameter("CPS.TGM/FULL-TELEGRAM.STRC")\
#                           .buildDataset()
#
#try:
#d2=DevicePropertyQuery.builder(spark)\
#                    .system("CMW")\
#                    .startTime("2018-12-07 00:00:00.000")\
#                    .endTime("2018-12-07 01:00:00.000")\
#                    .entity()\
#                    .parameter("RPMBB.UA23.RSF2.A12B2/SUB_51")\
#                    .buildDataset()
#except Exception as ex:
#    print(ex)



try:
   df = DevicePropertyQuery.builder(spark).system('CMW')\
    .startTime('2018-05-23 00:05:54.500').endTime('2018-05-23 00:06:54.500')\
    .entity().device('ZT10.QFO03').property('Acquisition').buildDataset()\
    .select('cyclestamp', 'current').orderBy('cyclestamp', ascending=False).collect()
except jpype.JavaException as ex:
    print(ex.message())
    print(ex.stacktrace())

for row in df:
    print(row.values()[0], row.values()[1])


