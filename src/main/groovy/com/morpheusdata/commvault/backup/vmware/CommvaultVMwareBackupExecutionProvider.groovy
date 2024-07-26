package com.morpheusdata.commvault.backup.vmware

import com.morpheusdata.commvault.backup.CommvaultBackupExecutionProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultVMwareBackupExecutionProvider extends CommvaultBackupExecutionProvider {

	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultVMwareBackupExecutionProvider(Plugin plugin, MorpheusContext morpheusContext, CommvaultBackupTypeProvider backupTypeProvider) {
		super(plugin, morpheusContext)
		this.backupTypeProvider = backupTypeProvider
	}


}
