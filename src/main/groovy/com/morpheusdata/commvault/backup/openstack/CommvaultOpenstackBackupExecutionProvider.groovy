package com.morpheusdata.commvault.backup.openstack

import com.morpheusdata.commvault.backup.CommvaultBackupExecutionProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultOpenstackBackupExecutionProvider extends CommvaultBackupExecutionProvider {

	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultOpenstackBackupExecutionProvider(Plugin plugin, MorpheusContext morpheusContext, CommvaultBackupTypeProvider backupTypeProvider) {
		super(plugin, morpheusContext)
		this.backupTypeProvider = backupTypeProvider
	}


}
