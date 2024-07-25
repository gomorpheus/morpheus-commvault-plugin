package com.morpheusdata.commvault.backup.file

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.backup.CommvaultBackupRestoreProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultFileBackupRestoreProvider extends CommvaultBackupRestoreProvider {

	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultFileBackupRestoreProvider(CommvaultPlugin plugin, MorpheusContext morpheusContext, CommvaultBackupTypeProvider backupTypeProvider) {
		super(plugin, morpheusContext)
		this.backupTypeProvider = backupTypeProvider
	}

}
