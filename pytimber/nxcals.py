import os
import getpass

import cmmnbuild_dep_manager

user_home= os.environ['HOME']
nxcals_home = os.path.join(user_home,'.nxcals')
keytab= os.path.join(nxcals_home,'keytab')
certs = os.path.join(nxcals_home,'nxcals_cacerts')
username=getpass.getuser()


class NXCals(object):
    def __init__(self,user=username,keytab=keytab,certs=certs):
        if os.path.isfile(keytab):
            self._keytab=keytab
        else:
            raise ValueError(f"Keytab file {keytab} does not exists")
        if os.path.isfile(certs):
            self._certs=certs
        else:
            raise ValueError(f"Certificate file {certs} does not exists")

        self._user=user
        import cmmnbuild_dep_manager
        self._mgr=cmmnbuild_dep_manager.Manager()
        self._jpype=self._mgr.start_jpype_jvm()
        self._org=self._jpype.JPackage('org')
        self._cern=self._jpype.JPackage('cern')
        self._java=self._jpype.java
        self._System=self._java.lang.System

        self.spark=self._get_spark()

        self.builders=self._cern.nxcals.data.access.builders
        self.builders.FluentQuery
        self.builders.KeyValuesQuery
        self.builders.QueryData
        self.builders.VariableQuery


    @property
    def DevicePropertyQuery(self):
        return self.builders.DevicePropertyQuery.builder(self.spark)

    @property
    def VariableQuery(self):
        return self.builders.VariableQuery.builder(self.spark)

    @property
    def KeyValuesQuery(self):
        return self.builders.KeyValuesQuery.builder(self.spark)

    def _get_spark(self):
        self._System.setProperty("spring.main.web-application-type", "none")
        self._System.setProperty("javax.net.ssl.trustStore", self._certs)
        self._System.setProperty("javax.net.ssl.trustStorePassword", "nxcals")
        self._System.setProperty("kerberos.keytab", self._keytab)
        self._System.setProperty("kerberos.principal", self._user)
        self._System.setProperty("service.url", "http://cs-ccr-nxcals6.cern.ch:19093")
        return self._cern.lhc.nxcals.util.NxcalsSparkSession.sparkSession()



