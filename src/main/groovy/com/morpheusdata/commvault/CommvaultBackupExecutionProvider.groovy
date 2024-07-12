package com.morpheusdata.commvault

import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.Workload
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultBackupExecutionProvider implements BackupExecutionProvider {

	CommvaultPlugin plugin
	MorpheusContext morpheusContext

	CommvaultBackupExecutionProvider(CommvaultPlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheusContext = morpheusContext
	}
	
	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Add additional configurations to a backup. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup provider.
	 * @param backupModel the current backup the configurations are applied to.
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Validate the configuration of the backup. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupModel the backup to validate
	 * @param config the original configuration supplied by external inputs.
	 * @param opts optional parameters used for
	 * @return a {@link ServiceResponse} object. The errors field of the ServiceResponse is used to send validation
	 * results back to the interface in the format of {@code errors['fieldName'] = 'validation message' }. The msg
	 * property can be used to send generic validation text that is not related to a specific field on the model.
	 * A ServiceResponse with any items in the errors list or a success value of 'false' will halt the backup creation
	 * process.
	 */
	@Override
	ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Create the backup resources on the external provider system.
	 * @param backupModel the fully configured and validated backup
	 * @param opts additional options used during backup creation
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
	 * creation on the external system failed and will halt any further backup creation processes in morpheus.
	 */
	@Override
	ServiceResponse createBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Delete the backup resources on the external provider system.
	 * @param backupModel the backup details
	 * @param opts additional options used during the backup deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Delete the results of a backup execution on the external provider system.
	 * @param backupResultModel the backup results details
	 * @param opts additional options used during the backup result deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * A hook into the execution process. This method is called before the backup execution occurs.
	 * @param backupModel the backup details associated with the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the execution preperation. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Provide additional configuration on the backup result. The backup result is a representation of the output of
	 * the backup execution including the status and a reference to the output that can be used in any future operations.
	 * @param backupResultModel
	 * @param opts
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.
	 */
	@Override
	ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Initiate the backup process on the external provider system.
	 * @param backup the backup details associated with the backup execution.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param executionConfig original configuration supplied for the backup execution.
	 * @param cloud cloud context of the target of the backup execution
	 * @param computeServer the target of the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution. A success value
	 * of 'false' will halt the execution process.
	 */
//	@Override
//	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
//		return ServiceResponse.success(new BackupExecutionResponse(backupResult))
//	}
	def getBackupProvider(Backup backup) {
		backup.backupProvider
	}

	def isCommvaultEnabled(backup) {
		getBackupProvider(backup)?.enabled
	}

	def getStoragePolicyId(Map authConfig, BackupJob backupJob) {
		def rtn = [success:false]
		try {
			def subclientId = backupJob.internalId
			def subclientResults = CommvaultBackupUtility.getSubclient(authConfig, subclientId)

			if(subclientResults.success && !subclientResults.errorCode) {
				rtn.storagePolicyId = subclientResults.subclient?.commonProperties?.storageDevice?.dataBackupStoragePolicy?.storagePolicyId
				rtn.success = true
			}
		} catch(e) {
			log.error("getStoragePolicyId error: ${e}", e)
		}

		return rtn
	}

	def getBackupsetId(Map authConfig, BackupJob backupJob) {
		def rtn = [success:false]
		try {
			def subclientId = backupJob.internalId
			def subclientResults = CommvaultBackupUtility.getSubclient(authConfig, subclientId)

			if(subclientResults.success && !subclientResults.errorCode) {
				rtn.backupsetId = subclientResults.subclient?.subClientEntity?.backupsetId
				rtn.success = true
			}
		} catch(e) {
			log.error("getBackupsetId error: ${e}", e)
		}

		return rtn
	}

	def getDefaultBackupSet(BackupProvider backupProvider, ReferenceData client) {
//		def backupSets = ReferenceData.where { category == "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}"}.list()
		def backupSets = morpheus.services.referenceData.list(new DataQuery()
				.withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}"))
		def rtn = backupSets.find { it.name == "defaultBackupSet" }

		return rtn
	}

	def initializeVmSubclient(Backup backup, Map opts) {
		def rtn = [success:false]
		try {
			def backupProvider = getBackupProvider(backup)
			def authConfig = plugin.getAuthConfig(backupProvider)
			def jobConfig = backup.backupJob.getConfigMap()
			def clientId = jobConfig.clientId
			def storagePolicyId = jobConfig.storagePolicyId ?: getStoragePolicyId(authConfig, backup.backupJob).storagePolicyId
			def backupSetId = jobConfig.backupsetId ?: getBackupsetId(authConfig, backup.backupJob).backupsetId
//			def client = ReferenceData.where { category == "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}" && internalId == clientId }.get()
			def client = morpheus.services.referenceData.find(new DataQuery()
												.withFilter("category", "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}")
												.withFilter("internalId", clientId))
//			def backupSet = backupSetId ? ReferenceData.where { category == "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}" && internalId == backupSetId }.get() : getDefaultBackupSet(backupProvider, client)
			def backupSetClient = morpheus.services.referenceData.find(new DataQuery()
														.withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}")
														.withFilter("internalId", backupSetId))
			def backupSet = backupSetId ? backupSetClient : getDefaultBackupSet(backupProvider, client)
//			def storagePolicy = ReferenceData.where { category == "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}" && internalId == storagePolicyId }.get()
			def storagePolicy = morpheus.services.referenceData.find(new DataQuery()
					.withFilter("category", "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")
					.withFilter("internalId", storagePolicyId))
//			def container = Container.get(backup.containerId)
			Workload workload = morpheus.services.workload.get(backup.containerId)
//			def server = workload.server
//			def server = container.server
			ComputeServer server = morpheus.services.computeServer.get(workload.server.id)
			rtn = CommvaultBackupUtility.createSubclient(authConfig, client.name, "${backup.name}-cvvm", [backupSet: backupSet, storagePolicy: storagePolicy])
			if(rtn.success && rtn?.subclientId) {
				def subclientResult = CommvaultBackupUtility.getSubclient(authConfig, rtn.subclientId)
//				backup.addConfigProperties([
//						"vmSubclientId": rtn.subclientId,  vmSubclientGuid: subclientResult?.subclient?.subClientEntity?.subclientGUID,
//						vmClientId: client.internalId, vmBackupsetId: backupSet?.internalId, vmStoragePolicyId: storagePolicy?.internalId
//				])
				backup.setConfigProperty("vmSubclientId", rtn.subclientId)
				backup.setConfigProperty("vmSubclientGuid", subclientResult?.subclient?.subClientEntity?.subclientGUID)
				backup.setConfigProperty("vmClientId", client.internalId)
				backup.setConfigProperty("vmBackupsetId", backupSet?.internalId)
				backup.setConfigProperty("vmStoragePolicyId", storagePolicy?.internalId)
//				backup.save(flush:true)
				morpheus.services.backup.save(backup)
				def vmClientId = (server.internalId ?: server.externalId) // use vmware internal ID, move this to the backup type service when split out.
				def vmClientName = server.name + "_" + server.externalId
				CommvaultBackupUtility.addVMToSubclient(authConfig, rtn.subclientId, vmClientId, vmClientName)
			}

		} catch(e) {
			log.error("initializeVmSubclient error: ${e}", e)
		}

		return rtn
	}

	def captureActiveSubclientBackup(authConfig, subclientId, clientId, backupsetId) {
		def activeJobsResults = CommvaultBackupUtility.getBackupJobs(authConfig, subclientId, [query: [clientId: clientId, backupsetId: backupsetId, jobFilter: "Backup", jobCategory: "Active"]])
		if(activeJobsResults.success && activeJobsResults.results.size() > 0) {
			def activeBackupJob = activeJobsResults.results.find { it.clientId == clientId && it.subclientId == subclientId && it.backupsetId == backupsetId }
			return ServiceResponse.success([backupJobId: activeBackupJob.backupJobId])
		}
	}

	private saveBackupResult(Backup backup, Map opts = [:]) {
		def status = opts.result ? getBackupStatus(opts.result) : "IN_PROGRESS"
		long sizeInMb = (opts.totalSize ?: 0) / 1048576
		opts.backupSetId = opts.backupSetId ?: createBackupResultSetId()

		def statusMap = [
				backupResultId: opts.backupResult.id,
				success: status != 'FAILED',
				status:status,
				sizeInMb:sizeInMb,
				backupSizeInMb: sizeInMb,
				externalId: opts.backupJobId,
				config: [
						accountId:backup.account.id,
						backupId:backup.id,
						backupName:backup.name,
						backupType:backup.backupType.code,
						serverId:backup.serverId,
						active:true,
						containerId:backup.containerId,
						instanceId:backup.instanceId,
						containerTypeId:backup.containerTypeId,
						restoreType:backup.backupType.restoreType,
						startDay:new Date().clearTime(),
						startDate:new Date(),
						id:createBackupResultId(),
						backupSetId:opts.backupSetId,
						backupSessionId:opts.backupSessionId,
						backupJobId: opts.backupJobId
				]
		]
		if(opts.error) {
			statusMap.status = "FAILED"
			statusMap.errorOutput = opts.error
		}

		updateBackupStatus(opts.backupResult.id, statusMap)
	}

	private getBackupStatus(backupState) {
		def status
		if(backupState.toLowerCase().contains("completed") && backupState.toLowerCase().contains("errors")) {
			status = BackupResult.Status.SUCCEEDED_WARNING
		} else if(backupState.contains("Failed") || backupState.contains("errors")) {
			status = BackupResult.Status.FAILED
		} else if(["Interrupted", "Killed", "Suspend", "Suspend Pending", "Kill Pending"].contains(backupState) || backupState.contains("Killed")) {
			status = BackupResult.Status.CANCELLED
		} else if(["Running", "Waiting", "Pending"].contains(backupState) || backupState.contains("Running")) {
			status = BackupResult.Status.IN_PROGRESS
		} else if(backupState == "Completed" || backupState.contains("Completed")) {
			status = BackupResult.Status.SUCCEEDED
		} else if(backupState == "Queued") {
			status = BackupResult.Status.START_REQUESTED
		} else if(["Kill", "Pending" ,"Interrupt", "Pending"].contains(backupState)) {
			status = BackupResult.Status.CANCEL_REQUESTED
		}

		return status ? status.toString() : status
	}

	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
		log.debug("executeBackup: {}", backup)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(!isCommvaultEnabled(backup)) {
//			return ServiceResponse.error("Commvault backup integration is disabled")
			rtn.error = "Commvault backup integration is disabled"
			return rtn
		}

		def results = [:]
		try {
			def backupProvider = getBackupProvider(backup)
			def authConfig = plugin.getAuthConfig(backupProvider)
			if(authConfig) {
				def subclientId = backup.getConfigProperty("vmSubclientId")
				if(!subclientId) {
					def vmSubclientResults = initializeVmSubclient(backup, opts)
					if(vmSubclientResults.success) {
						subclientId = backup.getConfigProperty("vmSubclientId")
					} else {
//						return ServiceResponse.error("Unable to execute backup ${backup.id}: Failed to create commvault subclient.")
						rtn.error = "Unable to execute backup ${backup.id}: Failed to create commvault subclient."
						return rtn
					}
				}
				results = CommvaultBackupUtility.backupSubclient(authConfig, subclientId)

				if(!results.success) {
					if(results.errorCode == 2) {
						def backupConfigMap = backup.getConfigMap()
						if(backupConfigMap) {
//							return captureActiveSubclientBackup(authConfig, backupConfigMap.vmSubclientId, backupConfigMap.vmClientId, backupConfigMap.vmBackupSetId)
							captureActiveSubclientBackup(authConfig, backupConfigMap.vmSubclientId, backupConfigMap.vmClientId, backupConfigMap.vmBackupSetId)
							rtn.success = true
							return rtn
						}
						if(!results.backupJobId) {
//							return ServiceResponse.error("Failed to capture active id for backup job ${backup.id.id}")
							rtn.error = "Failed to capture active id for backup job ${backup.id.id}"
							return rtn
						}
					} else {
						log.error("Failed to execute backup ${backup.id}")
//						return ServiceResponse.error("Failed to execute backup job ${backup.id}", results)
						rtn.error = "Failed to execute backup job ${backup.id}"
						return rtn
					}
					saveBackupResult(backup, results + [backupResult: backupConfig.backupResult, result: "FAILED"])

				} else {
					saveBackupResult(backup, results + [backupResult: backupConfig.backupResult])

				}
			}
		} catch(e) {
			log.error("executeBackup error: ${e}", e)
		}

//		return ServiceResponse.success(results)
		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {@link BackupStatusUtility} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		return ServiceResponse.success(new BackupExecutionResponse(backupResult))
	}
	
	/**
	 * Cancel the backup execution process without waiting for a result.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution cancellation.
	 */
	@Override
	ServiceResponse cancelBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Extract the results of a backup. This is generally used for packaging up a full backup for the purposes of
	 * a download or full archive of the backup.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup extraction.
	 */
	@Override
	ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

}		
