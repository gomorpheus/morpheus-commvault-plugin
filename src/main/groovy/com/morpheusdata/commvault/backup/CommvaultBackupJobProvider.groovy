package com.morpheusdata.commvault.backup

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.commvault.utils.CommvaultReferenceUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.Account
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * @author rahul.ray
 */

@Slf4j
class CommvaultBackupJobProvider implements BackupJobProvider {

    private CommvaultPlugin plugin
    MorpheusContext morpheusContext

    CommvaultBackupJobProvider(CommvaultPlugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    /**
     * Apply provider specific configurations to a {@link BackupJob}. The standard configurations are handled by Morpheus.
     * @param backupJobModel the backup job to apply the configuration changes to
     * @param config the configuration supplied by external inputs.
     * @param opts optional parameters used for configuration.
     * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
     * configuration and will halt the backup creation process.
     */
    @Override
    ServiceResponse configureBackupJob(BackupJob backupJobModel, Map config, Map opts) {
        ServiceResponse.success()
    }

    /**
     * Validate the configuration of the {@link BackupJob}. Morpheus will validate the backup based on the supplied option type
     * configurations such as required fields. Use this to either override the validation results supplied by the
     * default validation or to create additional validations beyond the capabilities of option type validation.
     * @param backupJobModel the backup job to validate
     * @param config the original configuration supplied by external inputs.
     * @param opts optional parameters used for
     * @return a {@link ServiceResponse} object. The errors field of the ServiceResponse is used to send validation
     * results back to the interface in the format of {@code errors['fieldName'] = 'validation message' }. The msg
     * property can be used to send generic validation text that is not related to a specific field on the model.
     * A ServiceResponse with any items in the errors list or a success value of 'false' will halt the backup job
     * creation process.
     */
    @Override
    ServiceResponse validateBackupJob(BackupJob backupJobModel, Map config, Map opts) {
        ServiceResponse.success()
    }

    /**
     * Create the {@link BackupJob} on the external provider system.
     * @param backupJob the fully configured and validated backup job
     * @param opts additional options used during backup job creation
     * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
     * creation on the external system failed and will halt any further processing in Morpheus.
     */
    @Override
    ServiceResponse createBackupJob(BackupJob backupJob, Map opts) {
        log.debug("createBackupJob {} and {} ", backupJob, opts)
        ServiceResponse response = ServiceResponse.prepare()
        try {
            def backupProvider = morpheusContext.services.backupProvider.get(backupJob.backupProvider.id)
            def authConfig = plugin.getAuthConfig(backupProvider)
            if (opts.commvaultClient) {
                // clear out the schedule so morpheus doesn't run the job
                backupJob.scheduleType = null
                backupJob.nextFire = null
                def client = morpheusContext.services.referenceData.get(opts.commvaultClient?.toLong())
                def backupSet
                if (opts.commvaultBackupSet) {
                    backupSet = morpheusContext.services.referenceData.get(opts.commvaultBackupSet?.toLong())
                } else {
                    backupSet = getDefaultBackupSet(backupProvider, client)
                }
                if (opts.commvaultStoragePolicy) {
                    def storagePolicy = morpheusContext.services.referenceData.get(opts.commvaultStoragePolicy?.toLong())
                    def jobName = backupJob.name + "-${backupJob.account.id}"
                    def result = CommvaultBackupUtility.createSubclient(authConfig, client.name, jobName, [backupSet: backupSet, storagePolicy: storagePolicy])
                    log.debug("createBackupJob: createSubclient result: " + result)
                    if (result?.subclientId) {
                        def subclientResult = CommvaultBackupUtility.getSubclient(authConfig, result.subclientId)
                        backupJob.externalId = subclientResult?.subclient?.subClientEntity?.subclientGUID
                        backupJob.internalId = result.subclientId
                        backupJob.setConfigProperty('subclientId', result.subclientId)
                        backupJob.setConfigProperty('clientId', client.internalId)
                        backupJob.setConfigProperty('backupsetId', backupSet.internalId)
                        backupJob.setConfigProperty('storagePolicyId', storagePolicy.internalId)
                        def savedBackupJob = morpheusContext.services.backupJob.create(backupJob)
                        response = ServiceResponse.success(savedBackupJob)
                    }
                } else {
                    response.msg = "Commvault storage policy ID not found."
                }
            } else {
                response.msg = "Commvault client ID not found."
            }
        } catch (e) {
            log.error("create Backup Job error: ${e}", e)
        }
        return response
    }

    /**
     * Clone the {@link BackupJob} on the external system.
     * @param sourceBackupJob the source backup job for cloning
     * @param backupJob the backup job that will be associated to the cloned backup job. The externalId of the
     *                       backup job should be set with the ID of the cloned job result.
     * @param opts additional options used during backup job clone
     * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
     * clone on the external system failed and will halt any further processing in Morpheus.
     */
    @Override
    ServiceResponse cloneBackupJob(BackupJob sourceBackupJob, BackupJob backupJob, Map opts) {
        log.debug("cloneBackupJob {}, {} and {}", sourceBackupJob, backupJob, opts)
        ServiceResponse response = ServiceResponse.prepare()
        try {
            def backupProvider = morpheusContext.services.backupProvider.get(backupJob.backupProvider.id)
            def sourceJobConfigMap = sourceBackupJob.getConfigMap()
            opts.commvaultClient = morpheusContext.async.referenceData.find(new DataQuery().withFilters(
                    [
                            new DataFilter('account.id', backupJob.account.id),
                            new DataFilter('category', "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}"),
                            new DataFilter('internalId', sourceJobConfigMap.clientId)
                    ]
            )).blockingGet()?.id
            opts.commvaultBackupSet = morpheusContext.async.referenceData.find(new DataQuery().withFilters(
                    [
                            new DataFilter('account.id', backupJob.account.id),
                            new DataFilter('category', "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${opts.commvaultClient}"),
                            new DataFilter('internalId', (sourceJobConfigMap.backupSetId ?: sourceJobConfigMap.backupsetId))
                    ]
            )).blockingGet()?.id
            opts.commvaultStoragePolicy = morpheusContext.async.referenceData.find(new DataQuery().withFilters(
                    [
                            new DataFilter('account.id', backupJob.account.id),
                            new DataFilter('category', "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}"),
                            new DataFilter('internalId', sourceJobConfigMap.storagePolicyId)
                    ]
            )).blockingGet()?.id

            response = createBackupJob(backupJob, opts)
            log.debug("cloneBackupJob: response: ${response}")
        } catch (e) {
            log.error("cloneBackupJob error: ${e}", e)
        }
        return response
    }

    /**
     * Add a backup to an existing backup job in the external system.
     * @param backupJobModel the backup job receiving the additional backup
     * @param opts additional options
     * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
     * operation on the external system failed and will halt any further processing in Morpheus.
     */
    @Override
    ServiceResponse addToBackupJob(BackupJob backupJobModel, Map opts) {
        ServiceResponse.success()
    }

    /**
     * Delete the backup job in the external system.
     * @param backupJobModel the backup job to be removed
     * @param opts additional options
     * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
     * operation on the external system failed and will halt any further processing in Morpheus.
     */
    @Override
    ServiceResponse deleteBackupJob(BackupJob backupJob, Map opts) {
        def rtn = [success:false]
        try {
            def backupProvider = backupJob.backupProvider
            def authConfig = plugin.getAuthConfig(backupProvider)

            rtn = CommvaultBackupUtility.deleteSubclient(authConfig, backupJob.internalId)
            if(!rtn.success) {
                def subclientResponse = CommvaultBackupUtility.getSubclient(authConfig, backupJob.internalId)
                if(!subclientResponse.success && subclientResponse.statusCode == 404) {
                    rtn.success = true
                }
            }
        } catch (Throwable t) {
            log.error(t.message, t)
            throw new RuntimeException("Unable to remove backup job:${t.message}", t)
        }
        return ServiceResponse.create(rtn)
    }

    /**
     * Execute the backup job on the external system.
     * @param backupJob the backup job to be executed
     * @param opts additional options
     * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
     * operation on the external system failed and will halt any further processing in Morpheus.
     */
    @Override
    ServiceResponse executeBackupJob(BackupJob backupJob, Map opts) {
        log.debug("executeBackupJob: {}, {}", backupJob, opts)
        ServiceResponse<List<BackupExecutionResponse>> rtn = ServiceResponse.prepare(new ArrayList<BackupExecutionResponse>())
        if(!backupJob.backupProvider?.enabled) {
            return ServiceResponse.error("Commvault backup integration is disabled")
        }

        def results
        try {
            def backupProvider = backupJob.backupProvider
            def authConfig = plugin.getAuthConfig(backupProvider)
            if(authConfig) {
                def subclientId = backupJob.internalId
                results = CommvaultBackupUtility.backupSubclient(authConfig, subclientId)
                log.debug("executeBackupJob result: {}", results)

                if(!results.success) {
                    if(results.errorCode == 2) {
                        // the job is already running, capture the job id
                        def jobConfig = backupJob?.getConfigMap()
                        if(jobConfig) {
                            return CommvaultBackupUtility.captureActiveSubclientBackup(authConfig, jobConfig.subclientId, jobConfig.clientId, jobConfig.backupsetId)
                        }
                        if(!results.backupJobId) {
                            return ServiceResponse.error("Failed to capture active id for backup job ${backupJob.id}")

                        }
                    } else {
                        log.error("Failed to execute backup job ${backupJob.id}")
                        return ServiceResponse.error("Failed to execute backup job ${backupJob.id}", results)
                    }
                }

                Account tmpAccount = opts.account ?: backupJob.account

                List<Backup> allJobBackups = morpheusContext.services.backup.list(new DataQuery()
                        .withFilter('account.id', tmpAccount.id)
                        .withFilter('backupJob.id', backupJob.id)
                )
                List<Backup> jobBackups = []
                allJobBackups.each { Backup backup ->
                    if (!opts.user || backup.createdBy?.id == opts.user.id) {
                        jobBackups << backup
                    }
                }

                jobBackups.each { backup ->
                    try {
                        BackupResult backupResult = new BackupResult(backup: backup)
                        backupResult.backupType = "default"
                        backupResult.backupSetId = results.backupSetId ?: BackupResultUtility.generateBackupResultSetId()
                        backupResult.externalId = results.backupJobId
                        backupResult.setConfigProperty("id", BackupResultUtility.generateBackupResultSetId())
                        backupResult.setConfigProperty("backupJobId", results.backupJobId)
                        backupResult.status = results.result ? CommvaultReferenceUtility.getBackupStatus(results.result) : BackupResult.Status.IN_PROGRESS
                        backupResult.sizeInMb = (results.totalSize ?: 0) / ComputeUtility.ONE_MEGABYTE

                        def executionResponse = new BackupExecutionResponse(backupResult)
                        executionResponse.updates = true
                        rtn.data.add(executionResponse)
                    } catch(Exception ex) {
                        log.error("Failed to create backup result backup ${backup.id}", ex)
                        return ServiceResponse.error("Failed to create backup result for backup ${backup.id}")
                    }
                }
                rtn.success = true
            } else {
                return ServiceResponse.error("Authentication information not found.")
            }
        } catch(e) {
            log.error("Failed to execute backup job ${backupJob.id}: ${e}", e)
            return ServiceResponse.error("Failed to execute backup job ${backupJob.id}")
        }
        return rtn
    }

    def getDefaultBackupSet(BackupProvider backupProvider, ReferenceData client) {
        def backupSets = morpheusContext.async.referenceData.listIdentityProjections(
                new DataQuery().withFilters([new DataFilter('category', "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${client.id}")])
        )
        def rtn = backupSets.find { it.name == "defaultBackupSet" }
        return rtn
    }
}
