package com.morpheusdata.commvault.backup.file

import com.morpheusdata.commvault.backup.CommvaultBackupExecutionProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultFileBackupExecutionProvider extends CommvaultBackupExecutionProvider {

	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultFileBackupExecutionProvider(Plugin plugin, MorpheusContext morpheusContext, CommvaultBackupTypeProvider backupTypeProvider) {
		super(plugin, morpheusContext)
		this.backupTypeProvider = backupTypeProvider
	}


}
