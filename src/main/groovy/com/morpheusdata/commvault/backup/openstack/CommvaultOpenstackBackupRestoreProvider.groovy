package com.morpheusdata.commvault.backup.openstack

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.backup.CommvaultBackupRestoreProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultOpenstackBackupRestoreProvider extends CommvaultBackupRestoreProvider {

	CommvaultBackupTypeProvider backupTypeProvider

	CommvaultOpenstackBackupRestoreProvider(CommvaultPlugin plugin, MorpheusContext morpheusContext, CommvaultBackupTypeProvider backupTypeProvider) {
		super(plugin, morpheusContext)
		this.backupTypeProvider = backupTypeProvider
	}

}
