package com.morpheusdata.commvault

import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.DateUtility
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

	private CommvaultPlugin plugin
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
		log.debug("configureBackup: {}, {}, {}", backup, config, opts)
		if(config.commvaultClient) {
			backup.setConfigProperty("commvaultClientId", config.commvaultClient)
		}
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
		log.debug("createBackup {}:{} to job {} with opts: {}", backup.id, backup.name, backup.backupJob.id, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupProvider = backup.backupProvider
			def authConfig = plugin.getAuthConfig(backupProvider)
			def backupJob = backup.backupJob
			def server
			if(backup.computeServerId) {
				server = morpheusContext.services.computeServer.get(backup.computeServerId)
			} else {
				def workload = morpheusContext.services.workload.get(backup.containerId)
				server = morpheusContext.services.computeServer.get(workload?.server.id)
			}
			if(server) {
				def subClientId = backupJob.internalId
				def vmClientId = (server.internalId ?: server.externalId) // use vmware internal ID, move this to the backup type service when split out.
				def vmClientName = server.name + "_" + server.externalId
				def results = CommvaultBackupUtility.addVMToSubclient(authConfig, subClientId, vmClientId, vmClientName)
				log.debug("results: ${results}")
				if (results.success == true) {
					rtn.success = true
				}
			}
		} catch(e) {
			log.error("createBackup error: ${e}", e)
		}
		return rtn
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
			log.info("RAZI :: initializeVmSubclient -> backupProvider: ${backupProvider}")
			def authConfig = plugin.getAuthConfig(backupProvider)
			log.info("RAZI :: initializeVmSubclient -> authConfig: ${authConfig}")
			def jobConfig = backup.backupJob.getConfigMap()
			log.info("RAZI :: initializeVmSubclient -> jobConfig: ${jobConfig}")
			def clientId = jobConfig.clientId
			log.info("RAZI :: initializeVmSubclient -> clientId: ${clientId}")
			def storagePolicyId = jobConfig.storagePolicyId ?: getStoragePolicyId(authConfig, backup.backupJob).storagePolicyId
			log.info("RAZI :: initializeVmSubclient -> storagePolicyId: ${storagePolicyId}")
			def backupSetId = jobConfig.backupsetId ?: getBackupsetId(authConfig, backup.backupJob).backupsetId
			log.info("RAZI :: initializeVmSubclient -> backupSetId: ${backupSetId}")
//			def client = ReferenceData.where { category == "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}" && internalId == clientId }.get()
			log.info("RAZI :: initializeVmSubclient -> client -> category: ${backupProvider.type.code}.backup.backupServer.${backupProvider.id}")
			def client = morpheus.services.referenceData.find(new DataQuery()
												.withFilter("category", "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}")
												.withFilter("internalId", clientId))
			log.info("RAZI :: initializeVmSubclient -> client: ${client}")
//			def backupSet = backupSetId ? ReferenceData.where { category == "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}" && internalId == backupSetId }.get() : getDefaultBackupSet(backupProvider, client)
			log.info("RAZI :: initializeVmSubclient -> backupSetClient -> category: ${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}")
			def backupSetClient = morpheus.services.referenceData.find(new DataQuery()
														.withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}")
														.withFilter("internalId", backupSetId))
			log.info("RAZI :: initializeVmSubclient -> backupSetClient: ${backupSetClient}")
			def backupSet = backupSetId ? backupSetClient : getDefaultBackupSet(backupProvider, client)
			log.info("RAZI :: initializeVmSubclient -> backupSet: ${backupSet}")
//			def storagePolicy = ReferenceData.where { category == "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}" && internalId == storagePolicyId }.get()
			log.info("RAZI :: initializeVmSubclient -> backupSetClient -> category: ${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")
			def storagePolicy = morpheus.services.referenceData.find(new DataQuery()
					.withFilter("category", "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")
					.withFilter("internalId", storagePolicyId))
			log.info("RAZI :: initializeVmSubclient -> storagePolicy: ${storagePolicy}")
//			def container = Container.get(backup.containerId)
			log.info("RAZI :: backup.containerId: ${backup.containerId}")
			Workload workload = morpheus.services.workload.get(backup.containerId)
			log.info("RAZI :: initializeVmSubclient -> workload: ${workload}")
			log.info("RAZI :: initializeVmSubclient -> workload.server: ${workload.server}")
			def server = workload.server
//			def server = container.server
//			ComputeServer server = morpheus.services.computeServer.get(workload.server.id)
			rtn = CommvaultBackupUtility.createSubclient(authConfig, client.name, "${backup.name}-cvvm", [backupSet: backupSet, storagePolicy: storagePolicy])
			log.info("RAZI :: initializeVmSubclient -> rtn: ${rtn}")
			if(rtn.success && rtn?.subclientId) {
				def subclientResult = CommvaultBackupUtility.getSubclient(authConfig, rtn.subclientId)
				log.info("RAZI :: initializeVmSubclient -> subclientResult: ${subclientResult}")
//				backup.addConfigProperties([
//						"vmSubclientId": rtn.subclientId,  vmSubclientGuid: subclientResult?.subclient?.subClientEntity?.subclientGUID,
//						vmClientId: client.internalId, vmBackupsetId: backupSet?.internalId, vmStoragePolicyId: storagePolicy?.internalId
//				])
				log.info("RAZI :: initializeVmSubclient -> vmSubclientId: ${rtn.subclientId}")
				log.info("RAZI :: subclientResult?.subclient: ${subclientResult?.subclient}")
				log.info("RAZI :: subclientResult?.subclient?.subClientEntity: ${subclientResult?.subclient?.subClientEntity}")
				log.info("RAZI :: initializeVmSubclient -> vmSubclientGuid: ${subclientResult?.subclient?.subClientEntity?.subclientGUID}")
				log.info("RAZI :: initializeVmSubclient -> vmClientId: ${client.internalId}")
				log.info("RAZI :: initializeVmSubclient -> vmBackupsetId: ${backupSet?.internalId}")
				log.info("RAZI :: initializeVmSubclient -> vmStoragePolicyId: ${storagePolicy?.internalId}")
				backup.setConfigProperty("vmSubclientId", rtn.subclientId)
				backup.setConfigProperty("vmSubclientGuid", subclientResult?.subclient?.subClientEntity?.subclientGUID)
				backup.setConfigProperty("vmClientId", client.internalId)
				backup.setConfigProperty("vmBackupsetId", backupSet?.internalId)
				backup.setConfigProperty("vmStoragePolicyId", storagePolicy?.internalId)
//				backup.save(flush:true)
				morpheus.services.backup.save(backup)
				def vmClientId = (server.internalId ?: server.externalId) // use vmware internal ID, move this to the backup type service when split out.
				log.info("RAZI :: initializeVmSubclient -> vmClientId: ${vmClientId}")
				def vmClientName = server.name + "_" + server.externalId
				log.info("RAZI :: initializeVmSubclient -> vmClientName: ${vmClientName}")
				CommvaultBackupUtility.addVMToSubclient(authConfig, rtn.subclientId, vmClientId, vmClientName)
				log.info("RAZI :: addVMToSubclient : SUCCESS")
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
//		opts.backupSetId = opts.backupSetId ?: createBackupResultSetId()
		opts.backupSetId = opts.backupSetId ?: BackupResultUtility.generateBackupResultSetId()

		def statusMap = [
				backupResultId: opts.backupResult.id,
				success: status != 'FAILED',
				status:status,
				sizeInMb:sizeInMb,
				backupSizeInMb: sizeInMb,
				externalId: opts.backupJobId,
				config: [
						accountId		: backup.account.id,
						backupId		: backup.id,
						backupName		: backup.name,
						backupType		: backup.backupType.code,
						serverId		: backup.serverId,
						active			: true,
						containerId		: backup.containerId,
						instanceId		: backup.instanceId,
						containerTypeId	: backup.containerTypeId,
						restoreType		: backup.backupType.restoreType,
						startDay		: new Date().clearTime(),
						startDate		: new Date(),
//						id:createBackupResultId(),
						id 				: BackupResultUtility.generateBackupResultSetId(),
						backupSetId		: opts.backupSetId,
						backupSessionId	: opts.backupSessionId,
						backupJobId		: opts.backupJobId
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
		log.info("RAZI :: backup: ${backup}")
		log.info("RAZI :: backupResult: ${backupResult}")
		log.info("RAZI :: executionConfig: ${executionConfig}")
		log.info("RAZI :: opts: ${opts}")
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		log.info("RAZI :: executeBackup -> rtn: ${rtn}")
		log.info("RAZI :: !isCommvaultEnabled(backup): ${!isCommvaultEnabled(backup)}")
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
				log.info("RAZI :: subclientId -> before if(!subclientId): ${subclientId}")
				if(!subclientId) {
					def vmSubclientResults = initializeVmSubclient(backup, opts)
					log.info("RAZI :: vmSubclientResults: ${vmSubclientResults}")
					if(vmSubclientResults.success) {
						subclientId = backup.getConfigProperty("vmSubclientId")
						log.info("RAZI :: subclientId -> inside if(!subclientId): ${subclientId}")
					} else {
//						return ServiceResponse.error("Unable to execute backup ${backup.id}: Failed to create commvault subclient.")
						rtn.error = "Unable to execute backup ${backup.id}: Failed to create commvault subclient."
						return rtn
					}
				}
				log.info("RAZI :: authConfig: ${authConfig}")
				log.info("RAZI :: subclientId: ${subclientId}")
				results = CommvaultBackupUtility.backupSubclient(authConfig, subclientId)
				log.info("RAZI :: executeBackup -> results: ${results}")

				if(!results.success) {
					if(results.errorCode == 2) {
						def backupConfigMap = backup.getConfigMap()
						log.info("RAZI :: backupConfigMap: ${backupConfigMap}")
						if(backupConfigMap) {
							def backupResponce = captureActiveSubclientBackup(authConfig, backupConfigMap.vmSubclientId, backupConfigMap.vmClientId, backupConfigMap.vmBackupSetId)
							log.info("RAZI :: backupResponce: ${backupResponce}")
//							return captureActiveSubclientBackup(authConfig, backupConfigMap.vmSubclientId, backupConfigMap.vmClientId, backupConfigMap.vmBackupSetId)
							return backupResponce
						}
						if(!results.backupJobId) {
//							return ServiceResponse.error("Failed to capture active id for backup job ${backup.id.id}")
							rtn.error = "Failed to capture active id for backup job ${backup.id.id}"
							return rtn
						}
					} else {
						log.error("Failed to execute backup ${backup.id}")
//						return ServiceResponse.error("Failed to execute backup job ${backup.id}", results)
//						rtn = results
                        log.info("RAZI :: results : if(results.errorCode == 2) -> else: ${results}")
						rtn.error = "Failed to execute backup job ${backup.id}"
						return rtn
					}
//					saveBackupResult(backup, results + [backupResult: backupConfig.backupResult, result: "FAILED"])
					rtn.data.backupResult.status = BackupResult.Status.FAILED
					rtn.data.updates = true

				} else {
//					saveBackupResult(backup, results + [backupResult: backupConfig.backupResult])
					rtn.success = true
					rtn.data.backupResult.backupSetId = results.backupSetId ?: BackupResultUtility.generateBackupResultSetId()
					rtn.data.backupResult.externalId = results.backupJobId
					rtn.data.backupResult.setConfigProperty("id", BackupResultUtility.generateBackupResultSetId())
					rtn.data.backupResult.setConfigProperty("backupJobId", results.backupJobId)
//					rtn.data.backupResult.setConfigProperty("vmId", snapshotResults.externalId)
//					rtn.data.backupResult.sizeInMb = (saveResults.archiveSize ?: 1) / ComputeUtility.ONE_MEGABYTE
					rtn.data.backupResult.status = results.result ? getBackupStatus(results.result) : "IN_PROGRESS"
					rtn.data.backupResult.sizeInMb = (results.totalSize ?: 0) / ComputeUtility.ONE_MEGABYTE
					rtn.data.updates = true
					if(!backupResult.endDate) {
						rtn.data.backupResult.endDate = new Date()
						def startDate = backupResult.startDate
						if(startDate) {
							def start = DateUtility.parseDate(startDate)
							def end = rtn.data.backupResult.endDate
							rtn.data.backupResult.durationMillis = end.time - start.time
						}
					}

				}
			}
		} catch(e) {
			log.error("executeBackup error: ${e}", e)
			rtn.data.backupResult.status = BackupResult.Status.FAILED
			rtn.data.updates = true
		}

//		return ServiceResponse.success(results)
		log.info("RAZI :: executeBackup -> END rtn: ${rtn}")
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
//	@Override
//	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
//		return ServiceResponse.success(new BackupExecutionResponse(backupResult))
//	}
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		def backup = backupResult.backup
        log.info("RAZI :: isCommvaultEnabled(backup): ${isCommvaultEnabled(backup)}")
		if(isCommvaultEnabled(backup)) {
			def backupProvider = getBackupProvider(backup)
			def authConfig = plugin.getAuthConfig(backupProvider)
            log.info("RAZI :: refreshBackupResult -> authConfig: ${authConfig}")
			def backupJobId = backupResult.externalId ?: backupResult.getConfigProperty('backupJobId')
            log.info("RAZI :: backupJobId: ${backupJobId}")
			def backupJob = backup.backupJob

            log.info("RAZI :: if(!backupJob && backupJobId): ${!backupJob && backupJobId}")
			if(!backupJob && backupJobId) {
				def result = CommvaultBackupUtility.getJob(authConfig, backupJobId)
                log.info("RAZI :: refreshBackupResult -> result:${result}")
				backupJob = result.result
			}

            log.info("RAZI :: refreshBackupResult -> backupJob: ${backupJob}")
			if(backupJob) {
				def isVmSubclient = backupResult.backup.getConfigProperty("vmSubclientId") != null
                log.info("RAZI :: isVmSubclient: ${isVmSubclient}")
                log.info("RAZI :: backupJob.endTime: ${backupJob.endTime}")
                log.info("RAZI :: !backupJob.parentJobId: ${!backupJob.parentJobId}")
				if(backupJob.endTime != "0" && !backupJob.parentJobId && !isVmSubclient) {
//					def container = Container.get(backupResult.containerId)
					Workload workload = morpheus.services.workload.get(backup.containerId)
                    log.info("RAZI :: refreshBackupResult -> workload: ${workload}")
					def vmGuid = workload.server?.internalId
                    log.info("RAZI :: refreshBackupResult -> vmGuid: ${vmGuid}")
					// if(vmGuid) {
					// 	def vmJobResult = CommvaultBackupUtility.getVMJobForParentJob(authConfig, vmGuid, backupJob.externalId, [endTime: backupJob.endTime])
					// 	if(vmJobResult.result) {
					// 		backupJob = vmJobResult.result
					// 	}
					// }
				}

//				def result = [:]
//				result.status = getBackupStatus(backupJob.result)
				rtn.data.backupResult.status = getBackupStatus(backupJob.result)
                log.info("RAZI :: rtn.data.backupResult.status: ${rtn.data.backupResult.status}")
//				long sizeInMb = (backupJob.totalSize ?: 0 )/ 1048576
//				result.sizeInMb = sizeInMb
				rtn.data.backupResult.sizeInMb = (backupJob.totalSize ?: 0 )/ ComputeUtility.ONE_MEGABYTE
                log.info("RAZI :: rtn.data.backupResult.sizeInMb: ${rtn.data.backupResult.sizeInMb}")
//				result.backupSizeInMb = sizeInMb
				def startDate = backupJob.startTime
				def endDate = backupJob.endTime
				if(startDate && endDate) {
					def start = startDate.toLong() * 1000
					def end = endDate.toLong() * 1000
//					result.startDate = start ? new Date(start) : null
					rtn.data.backupResult.startDate = start ? new Date(start) : null
//					result.endDate = end ? new Date(end) : null
					rtn.data.backupResult.endDate = end ? new Date(end) : null
//					result.durationMillis = (start && end) ? (end - start) : 0
					rtn.data.backupResult.durationMillis = (start && end) ? (end - start) : 0
				}
                log.info("RAZI :: backupJob.parentJobId: ${backupJob.parentJobId}")
				if(backupJob.parentJobId) {
//					result.config = result.config ?: [:]
					rtn.data.backupResult.setConfigProperty("backupJobId", backupJob.externalId)
					rtn.data.backupResult.setConfigProperty("parentJobId", backupJob.parentJobId)
//					result.config.backupJobId = backupJob.externalId
//					result.config.parentJobId = backupJob.parentJobId
				}
//				backupService.updateBackupStatus(backupResult.id, null, result)
			}
			logoutSession(authConfig)
            log.info("RAZI :: logoutSession(authConfig) : SUCCESS")
		}
        log.info("RAZI :: refreshBackupResult -> last rtn: ${rtn}")
		return rtn
	}

	def logoutSession(Map authConfig) {
		authConfig.token = authConfig.token ?: CommvaultBackupUtility.getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		logoutSession(authConfig.apiUrl, authConfig.token)
	}

	def logoutSession(String apiUrl, String token) {
        log.info("RAZI :: logoutSession -> token: ${token}")
		if(token) {
			CommvaultBackupUtility.logout(apiUrl, token)
		}
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
