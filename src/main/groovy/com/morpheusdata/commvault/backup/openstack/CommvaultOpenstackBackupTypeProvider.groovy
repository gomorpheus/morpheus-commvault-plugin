package com.morpheusdata.commvault.backup.openstack

import com.morpheusdata.commvault.backup.CommvaultBackupExecutionProvider
import com.morpheusdata.commvault.backup.CommvaultBackupRestoreProvider
import com.morpheusdata.commvault.backup.CommvaultBackupTypeProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.core.backup.BackupTypeProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * Provides a standard set of methods for a {@link com.morpheusdata.core.backup.BackupProvider}. A backup provider is the primary connection to the
 * external provider services. The backup provider supplies providers for provision types and/or container types via
 * the {@link BackupTypeProvider BackupTypeProviders} implemented within the provider.
 */
@Slf4j
class CommvaultOpenstackBackupTypeProvider extends CommvaultBackupTypeProvider {

	BackupExecutionProvider executionProvider
	BackupRestoreProvider restoreProvider

	CommvaultOpenstackBackupTypeProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return "commvaultOpenstackBackup"
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return "Commvault OpenStack Backup"
	}
	
	/**
	 * get the type of container compatible with this backup type
	 * @return the container type code
	 */
	@Override
	String getContainerType() {
		return "single"
	}
	
	/**
	 * Determines if this backup type supports copying the backup to a datastore (export).
	 * @return boolean indicating copy to store is supported
	 */
	@Override
	Boolean getCopyToStore() {
		return false
	}

	/**
	 * Determines if this backup type supports downloading the backups for this backup type.
	 * @return boolean indicating downloading is enabled
	 */
	@Override
	Boolean getDownloadEnabled() {
		return false
	}

	/**
	 * Determines if this backup type supports restoring a backup to the existing workload.
	 * @return boolean indicating restore to existing is enabled
	 */
	@Override
	Boolean getRestoreExistingEnabled() {
		return true
	}

	/**
	 * Determines if this backup type supports restoring to a new workload rather than replacing the existing workload.
	 * @return boolean indicating restore to new is enabled
	 */
	@Override
	Boolean getRestoreNewEnabled() {
		return true
	}

	/**
	 * Indicates the type of restore supported. Current options include: new, existing, online, offline, migration, failover
	 * @return the supported restore type
	 */
	@Override
	String getRestoreType() {
		return "online"
	}

	/**
	 * Get the desired method of restoring a backup to a new instance.
	 * <p>
	 * Available options:
	 * 		<ul>
	 * 			<li>
	 * 			 	DEFAULT -- Uses the backup as an input to the instance provision process. Generally this
	 * 			 				involves using a snapshot or image ID as the image used for provisioning, but the precise
	 * 			 				details are left up to the backup provider.
	 * 			</li>
	 * 		 	<li>
	 * 		 	    VM_RESTORE -- The external backup provider restores to a VM and the core system will associate the resulting
	 * 		 	    				VM to the internal resources.
	 * 		 	</li>
	 * 		 	<li>
	 * 		 	  	TEMP_EXTRACT -- determines the visibility of the restore to new option
	 * 		 	</li>
	 * 		</ul>
	 *
	 * @return
	 */
	@Override
	String getRestoreNewMode() {
		return "VM_RESTORE"
	}
	
	/**
	 * Does this backup type provider support copy to store?
	 * @return boolean indicating copy to store support
	 */
	@Override
	Boolean getHasCopyToStore() {
		return false
	}

	/**
	 * A list of {@link OptionType OptionTypes} for use in the backup create and edit forms.
	 * @return a list of option types
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		return new ArrayList<OptionType>()
	}

	/**
	 * Get the backup provider which will be responsible for all the operations related to backup executions.
	 * @return a {@link BackupExecutionProvider} providing methods for backup execution.
	 */
	@Override
	CommvaultBackupExecutionProvider getExecutionProvider() {
		if(!this.executionProvider) {
			this.executionProvider = new CommvaultOpenstackBackupExecutionProvider(plugin, morpheus, this)
		}
		return this.executionProvider
	}

	/**
	 * Get the backup provider which will be responsible for all the operations related to backup restore.
	 * @return a {@link BackupRestoreProvider} providing methods for backup restore operations.
	 */
	@Override
	CommvaultBackupRestoreProvider getRestoreProvider() {
		if(!this.restoreProvider) {
			this.restoreProvider = new CommvaultOpenstackBackupRestoreProvider(plugin, morpheus, this)
		}
		return this.restoreProvider
	}

}			
