package com.morpheusdata.commvault.sync

import com.morpheusdata.commvault.CommvaultBackupProvider
import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.BackupJobIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class SubclientsSync {
    private CommvaultPlugin plugin
    private MorpheusContext morpheusContext
    private CommvaultBackupProvider commvaultBackupProvider
    private BackupProvider backupProviderModel

    public SubclientsSync(CommvaultBackupProvider commvaultBackupProvider, BackupProvider backupProviderModel, CommvaultPlugin plugin) {
        this.commvaultBackupProvider = commvaultBackupProvider
        this.backupProviderModel = backupProviderModel
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("SubclientsSync execute")
            Map authConfig = plugin.getAuthConfig(backupProviderModel)
            def refDataList = morpheusContext.async.referenceData.list(new DataQuery()
                    .withFilter('account', backupProviderModel.account)
                    .withFilter('category', "${backupProviderModel.type.code}.backup.backupServer.${backupProviderModel.id}")
            ).toList().blockingGet()
            log.info("RAZI :: refDataList: ${refDataList}")
            def listResults = CommvaultBackupUtility.listSubclients(authConfig, refDataList)
            log.info("RAZI :: listResults: ${listResults}")
            if(listResults.success) {
                ArrayList<Map> cloudItems = listResults.subclients.findAll { !it.name.endsWith("-cvvm") }
                log.info("RAZI :: cloudItems: ${cloudItems}")
                Observable<BackupJobIdentityProjection> existingItems = morpheusContext.async.backupJob.listIdentityProjections(backupProviderModel)
                log.info("RAZI :: existingItems: ${existingItems}")
                SyncTask<BackupJobIdentityProjection, ArrayList<Map>, BackupJob> syncTask = new SyncTask<>(existingItems, cloudItems)
                log.info("RAZI :: syncTask: ${syncTask}")
                syncTask.addMatchFunction { BackupJobIdentityProjection domainObject, Map cloudItem ->
                    log.info("RAZI :: domainObject.externalId: ${domainObject.externalId}")
                    log.info("RAZI :: cloudItem.externalId: ${cloudItem.externalId}")
                    domainObject.externalId == cloudItem.externalId
                }.onDelete { List<BackupJobIdentityProjection> removeItems ->
                    log.info("RAZI :: calling deleteSubclients")
                    deleteSubclients(removeItems)
                    log.info("RAZI :: calling deleteSubclients COMPLETED")
                }.onUpdate { List<SyncTask.UpdateItem<BackupJob, Map>> updateItems ->
                    log.info("RAZI :: calling updateMatchedSubclients")
                    updateMatchedSubclients(updateItems)
                    log.info("RAZI :: calling updateMatchedSubclients COMPLETED")
                }.onAdd { itemsToAdd ->
                    log.info("RAZI :: calling addMissingSubclients")
                    addMissingSubclients(itemsToAdd)
                    log.info("RAZI :: calling addMissingSubclients COMPLETED")
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<BackupJobIdentityProjection, Map>> updateItems ->
                    return morpheusContext.async.backupJob.list( new DataQuery(backupProviderModel.account).withFilter("id", 'in', updateItems.collect { it.existingItem.id }))
                }.start()
            } else {
                log.error("Error listing subclients")
                return Single.just(false).toObservable()
            }
        } catch(Exception ex) {
            log.error("Subclients Sync error: {}", ex, ex)
        }
    }

    private deleteSubclients(List<BackupJobIdentityProjection> removeItems) {
//        log.debug "deleteSubclients: ${removeItems}"
        log.info("RAZI :: removeItems: ${removeItems}")
        morpheusContext.async.resourcePermission.remove(removeItems).subscribe().dispose()
        morpheusContext.async.backupJob.remove(removeItems).subscribe().dispose()
        log.info("RAZI :: deleteSubclients SUCCESS")
    }

    private updateMatchedSubclients(List<SyncTask.UpdateItem<BackupJob, Map>> updateItems) {
//        log.debug "updateMatchedSubclients"
        log.info("RAZI :: updateItems: ${updateItems}")
        for(SyncTask.UpdateItem<BackupJob, Map> update in updateItems) {
            Map masterItem = update.masterItem
            BackupJob existingItem = update.existingItem

            Boolean doSave = false
            def jobName = masterItem.name.replace("-${existingItem.account.id}", "")
            if (existingItem.name != jobName) {
                existingItem.name = jobName
                doSave = true
            }

            if(existingItem.getConfigProperty('storagePolicyId') != masterItem.storagePolicyId) {
                existingItem.setConfigProperty('storagePolicyId', masterItem.storagePolicyId)
                doSave = true
            }

            if(existingItem.getConfigProperty('storagePolicyName') != masterItem.storagePolicyName) {
                existingItem.setConfigProperty('storagePolicyName', masterItem.storagePolicyName)
                doSave = true
            }

            if(existingItem.getConfigProperty('backupsetId') != masterItem.backupsetId) {
                existingItem.setConfigProperty('backupsetId', masterItem.backupsetId)
                doSave = true
            }

            if(existingItem.getConfigProperty('backupsetName') != masterItem.backupsetName) {
                existingItem.setConfigProperty('backupsetName', masterItem.backupsetName)
                doSave = true
            }
            log.info("RAZI :: doSave: ${doSave}")
            if (doSave == true) {
                log.debug "updating subclients!! ${existingItem.name}"
                morpheusContext.async.backupJob.save(existingItem).blockingGet()
            }
        }
    }

    private addMissingSubclients(itemsToAdd) {
//        log.debug "addMissingBackupJobs: ${itemsToAdd}"
        log.info("RAZI :: itemsToAdd: ${itemsToAdd}")

        def adds = []
        def objCategory = "commvault.job.${backupProviderModel.id}"
        log.info("RAZI :: objCategory: ${objCategory}")
        for(cloudItem in itemsToAdd) {
            def addConfig = [account:backupProviderModel.account, backupProvider:backupProviderModel, code:objCategory + '.' + cloudItem.externalId,
                             category:objCategory, name:cloudItem.name, externalId:cloudItem.externalId,
                             source:'commvault', enabled: cloudItem.backupEnabled, internalId: cloudItem.internalId, enabled: true
            ]
            log.info("RAZI :: addConfig: ${addConfig}")
            def add = new BackupJob(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        log.info("RAZI :: adds.size(): ${adds.size()}")
        if(adds.size() > 0) {
            log.debug "adding subclients: ${adds}"
            BulkCreateResult<BackupJob> result =  morpheusContext.async.backupJob.bulkCreate(adds).blockingGet()
            log.info("RAZI :: result: ${result}")
            if(!result.success) {
                log.error "Error adding subclients: ${result.errorCode} - ${result.msg}"
            }
        }
    }
}
