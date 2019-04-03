#kinit -f -r 5d -kt ~/.keytab rdemaria

import cmmnbuild_dep_manager

mgr=cmmnbuild_dep_manager.Manager()

mgr.register('pytimber')
mgr.resolve()

