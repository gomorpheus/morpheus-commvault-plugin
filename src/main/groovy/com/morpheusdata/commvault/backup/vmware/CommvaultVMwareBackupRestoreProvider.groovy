package com.morpheusdata.commvault.backup.vmware

import com.morpheusdata.commvault.backup.CommvaultBackupRestoreProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultVMwareBackupRestoreProvider extends CommvaultBackupRestoreProvider {

	Plugin plugin
	MorpheusContext morpheus
	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultVMwareBackupRestoreProvider(Plugin plugin, MorpheusContext morpheus, CommvaultBackupTypeProvider backupTypeProvider) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.backupTypeProvider = backupTypeProvider
	}

}
