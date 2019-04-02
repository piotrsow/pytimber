import cmmnbuild_dep_manager

mgr=cmmnbuild_dep_manager.Manager()

#mgr.register('pytimber')
#mgr.resolve()

jpype=mgr.start_jpype_jvm()

org=jpype.JPackage('org')
cern=jpype.JPackage('cern')
java=jpype.java
System=java.lang.System

#System.setProperty("logging.config", "classpath:log4j2.yml")
System.setProperty("javax.net.ssl.trustStore", "nxcals_cacerts")
System.setProperty("javax.net.ssl.trustStorePassword", "nxcals")
USER_HOME = System.getProperty("user.home")
System.setProperty("kerberos.keytab", USER_HOME + "/.keytab")
USER = System.getProperty("user.name")
System.setProperty("kerberos.principal", USER)
System.setProperty("service.url", "http://cs-ccr-testbed2.cern.ch:19093,http://cs-ccr-testbed3.cern.ch:19093,http://cs-ccr-nxcalstbs1.cern.ch:19093")
spark=cern.lhc.nxcals.util.NxcalsSparkSession.sparkSession()





