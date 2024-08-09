package com.morpheusdata.commvault.backup

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultApiUtility
import com.morpheusdata.commvault.utils.CommvaultReferenceUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
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
				def results = CommvaultApiUtility.addVMToSubclient(authConfig, subClientId, vmClientId, vmClientName)
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
		log.debug("deleteBackup :: backup: {}, opts: {}", backup, opts)
		def rtn = [success:false]
		try {
			def backupProvider = backup.backupProvider
			def authConfig = plugin.getAuthConfig(backupProvider)

			def workload = morpheusContext.services.workload.get(backup.containerId)
			def server = backup.containerId ? workload.server : null
			if(server) {
				def subclientId = backup.backupJob?.internalId
				if(subclientId) {
					def subclientResults = CommvaultApiUtility.getSubclient(authConfig, subclientId)
					if(subclientResults.success) {
						def subclientVM = subclientResults?.subclient?.vmContent?.children?.find { it.name == server.externalId || it.name == server.internalId }
						if(subclientVM) {
							rtn = CommvaultApiUtility.removeVMFromSubclient(authConfig, subclientId, subclientVM.name, backup.name)
							if(rtn.errorCode && !rtn.success) {
								rtn.success = true //this means its probably not found
							}
						} else {
							rtn.success = true
						}
					} else if(rtn.statusCode == 404) {
						rtn.success = true
						return ServiceResponse.create(rtn)
					}
				}

				// delete the on-demand backup subclient
				def vmSubclientId = backup.getConfigProperty("vmSubclientId")
				if(vmSubclientId) {
					CommvaultApiUtility.deleteSubclient(authConfig, vmSubclientId)
				}

				CommvaultApiUtility.deleteVMClient(authConfig, server.internalId)
			} else {
				rtn.success = true
				rtn.msg = "Could not find source resource"
			}
		} catch (Throwable t) {
			log.error(t.message, t)
			throw new RuntimeException("Unable to remove backup:${t.message}", t)
		}
		return ServiceResponse.create(rtn)
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
		log.debug("deleting backup result {}", backupResult)
		def rtn = [success:false]

		def backupProvider = backupResult.backup?.backupProvider
		def authConfig = plugin.getAuthConfig(backupProvider)
		def storagePolicyId = backupResult.backup?.backupJob?.getConfigProperty('storagePolicyId')

		def storagePolicy = morpheusContext.services.referenceData.find(new DataQuery()
				.withFilter("category", "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")
				.withFilter("externalId", storagePolicyId))

		def backupJobId = backupResult.externalId ?: backupResult.getConfigProperty('backupJobId')

		// con't delete job if active vm's were backed up on the same job
		def backupResultlist = morpheusContext.services.backup.backupResult.list(new DataQuery()
				.withFilter("externalId", "!=", null)
				.withFilter("externalId", backupJobId)
				.withFilter("id", "!=", backupResult.id))

		def sharedSubclient = backupJobId ? backupResultlist.size() > 0 : false
		if(backupJobId && storagePolicy && !sharedSubclient) {
			rtn = CommvaultApiUtility.deleteJob(authConfig, backupJobId, [storagePolicyName: storagePolicy.getConfigProperty("name"), storagePolicyCopyName: storagePolicy.getConfigProperty("copyName")])

		} else {
			rtn.success = true
		}

		return ServiceResponse.create(rtn)
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

	def getStoragePolicyId(Map authConfig, BackupJob backupJob) {
		def rtn = [success:false]
		try {
			def subclientId = backupJob.internalId
			def subclientResults = CommvaultApiUtility.getSubclient(authConfig, subclientId)

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
			def subclientResults = CommvaultApiUtility.getSubclient(authConfig, subclientId)

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
		def backupSets = morpheus.services.referenceData.list(new DataQuery()
				.withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}"))
		def rtn = backupSets.find { it.name == "defaultBackupSet" }

		return rtn
	}

	def initializeVmSubclient(Backup backup, Map opts) {
		def rtn = [success:false]
		try {
			def backupProvider = backup.backupProvider
			def authConfig = plugin.getAuthConfig(backupProvider)
			def jobConfig = backup.backupJob.getConfigMap()
			def clientId = jobConfig.clientId
			def storagePolicyId = jobConfig.storagePolicyId ?: getStoragePolicyId(authConfig, backup.backupJob).storagePolicyId
			def backupSetId = jobConfig.backupsetId ?: getBackupsetId(authConfig, backup.backupJob).backupsetId
			def client = morpheus.services.referenceData.find(new DataQuery()
												.withFilter("category", "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}")
												.withFilter("internalId", clientId))
			def backupSetClient = morpheus.services.referenceData.find(new DataQuery()
														.withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}")
														.withFilter("internalId", backupSetId))
			def backupSet = backupSetId ? backupSetClient : getDefaultBackupSet(backupProvider, client)
			def storagePolicy = morpheus.services.referenceData.find(new DataQuery()
					.withFilter("category", "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")
					.withFilter("internalId", storagePolicyId))
			Workload workload = morpheus.services.workload.get(backup.containerId)
			def server = workload.server
			rtn = CommvaultApiUtility.createSubclient(authConfig, client.name, "${backup.name}-cvvm", [backupSet: backupSet, storagePolicy: storagePolicy])
			if(rtn.success && rtn?.subclientId) {
				def subclientResult = CommvaultApiUtility.getSubclient(authConfig, rtn.subclientId)

				backup.setConfigProperty("vmSubclientId", rtn.subclientId)
				backup.setConfigProperty("vmSubclientGuid", subclientResult?.subclient?.subClientEntity?.subclientGUID)
				backup.setConfigProperty("vmClientId", client.internalId)
				backup.setConfigProperty("vmBackupsetId", backupSet?.internalId)
				backup.setConfigProperty("vmStoragePolicyId", storagePolicy?.internalId)

				morpheus.services.backup.save(backup)
				def vmClientId = (server.internalId ?: server.externalId) // use vmware internal ID, move this to the backup type service when split out.
				def vmClientName = server.name + "_" + server.externalId
				CommvaultApiUtility.addVMToSubclient(authConfig, rtn.subclientId, vmClientId, vmClientName)
			}

		} catch(e) {
			log.error("initializeVmSubclient error: ${e}", e)
		}

		return rtn
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
	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
		log.debug("executeBackup: {}", backup)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(!backup.backupProvider?.enabled) {
			rtn.error = "Commvault backup integration is disabled"
			return rtn
		}

		def results = [:]
		try {
			def backupProvider = backup.backupProvider
			def authConfig = plugin.getAuthConfig(backupProvider)
			if(authConfig) {
				def subclientId = backup.getConfigProperty("vmSubclientId")
				if(!subclientId) {
					def vmSubclientResults = initializeVmSubclient(backup, opts)
					if(vmSubclientResults.success) {
						subclientId = backup.getConfigProperty("vmSubclientId")
					} else {
						rtn.error = "Unable to execute backup ${backup.id}: Failed to create commvault subclient."
						return rtn
					}
				}

				results = CommvaultApiUtility.backupSubclient(authConfig, subclientId)

				if(!results.success) {
					if(results.errorCode == 2) {
						def backupConfigMap = backup.getConfigMap()
						if(backupConfigMap) {
							return CommvaultApiUtility.captureActiveSubclientBackup(authConfig, backupConfigMap.vmSubclientId, backupConfigMap.vmClientId, backupConfigMap.vmBackupSetId)
						}
						if(!results.backupJobId) {
							rtn.error = "Failed to capture active id for backup job ${backup.id.id}"
							return rtn
						}
					} else {
						log.error("Failed to execute backup job ${backup.id}")
						rtn.error = "Failed to execute backup job ${backup.id}"
						return rtn
					}
					rtn.data.backupResult.status = BackupResult.Status.FAILED
					rtn.data.updates = true

				} else {
					rtn.success = true
					rtn.data.backupResult.backupSetId = results.backupSetId ?: BackupResultUtility.generateBackupResultSetId()
					rtn.data.backupResult.externalId = results.backupJobId
					rtn.data.backupResult.setConfigProperty("id", BackupResultUtility.generateBackupResultSetId())
					rtn.data.backupResult.setConfigProperty("backupJobId", results.backupJobId)
					rtn.data.backupResult.status = results.result ? CommvaultReferenceUtility.getBackupStatus(results.result) : BackupResult.Status.IN_PROGRESS
					rtn.data.backupResult.sizeInMb = (results.totalSize ?: 0) / ComputeUtility.ONE_MEGABYTE
					rtn.data.updates = true

				}
			}
		} catch(e) {
			log.error("executeBackup error: ${e}", e)
			rtn.data.backupResult.status = BackupResult.Status.FAILED
			rtn.data.updates = true
		}

		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {BackupStatusUtility} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		log.debug("refreshBackupResult -> backupResult: {}", backupResult)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		def backup = backupResult.backup
		if(backup.backupProvider?.enabled) {
			def backupProvider = backup.backupProvider
			def authConfig = plugin.getAuthConfig(backupProvider)
			def backupJobId = backupResult.externalId ?: backupResult.getConfigProperty('backupJobId')
			Map backupJob = null

			if(!backupJob && backupJobId) {
				def result = CommvaultApiUtility.getJob(authConfig, backupJobId)
				backupJob = result.result
			}

			if(backupJob) {
				rtn.data.backupResult.status = CommvaultReferenceUtility.getBackupStatus(backupJob.result)
				rtn.data.backupResult.sizeInMb = (backupJob.totalSize ?: 0 )/ ComputeUtility.ONE_MEGABYTE
				def startDate = backupJob.startTime
				def endDate = backupJob.endTime
				if(startDate && endDate) {
					def start = startDate.toLong() * 1000
					def end = endDate.toLong() * 1000
					rtn.data.backupResult.startDate = start ? new Date(start) : null
					rtn.data.backupResult.endDate = end ? new Date(end) : null
					rtn.data.backupResult.durationMillis = (start && end) ? (end - start) : 0
				}
				if(backupJob.parentJobId) {
					rtn.data.backupResult.setConfigProperty("backupJobId", backupJob.externalId)
					rtn.data.backupResult.setConfigProperty("parentJobId", backupJob.parentJobId)
				}
				rtn.data.updates = true
				rtn.success = true

			}
			logoutSession(authConfig)
		}
		return rtn
	}

	def logoutSession(Map authConfig) {
		logoutSession(authConfig.apiUrl, authConfig.token)
	}

	def logoutSession(String apiUrl, String token) {
		if(token) {
			CommvaultApiUtility.logout(apiUrl, token)
		}
	}

	/**
	 * Cancel the backup execution process without waiting for a result.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution cancellation.
	 */
	@Override
	ServiceResponse cancelBackup(BackupResult backupResult, Map opts) {
		log.debug("cancelBackup: backupResult: {}, opts {}:", backupResult, opts)
		ServiceResponse response = ServiceResponse.prepare()
		if(backupResult != null) {
			try {
				def backupProvider = backupResult.backup.backupProvider
				def authConfig = plugin.getAuthConfig(backupProvider)
				def backupJobId = backupResult.externalId ?: backupResult.getConfigProperty("backupJobId")

				def result = CommvaultApiUtility.killBackupJob(authConfig, backupJobId)
				log.debug("cancelBackup : result: ${result}")
				if (authConfig.token) {
					CommvaultApiUtility.logout(authConfig.apiUrl, authConfig.token)
				}
				response.success = result.success
			} catch(e) {
				log.error("cancelBackup error: ${e}", e)
			}
		}
		return response
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
