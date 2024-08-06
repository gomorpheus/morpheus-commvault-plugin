package com.morpheusdata.commvault.sync

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultApiUtility
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.projection.BackupJobIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class SubclientsSync {
    private CommvaultPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel

    SubclientsSync(BackupProvider backupProviderModel, CommvaultPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("SubclientsSync execute")
            Map authConfig = plugin.getAuthConfig(backupProviderModel)

            def refDataList = morpheusContext.async.referenceData.list(new DataQuery()
                    .withFilter('account.id', backupProviderModel.account.id)
                    .withFilter('category', "${backupProviderModel.type.code}.backup.backupServer.${backupProviderModel.id}")
            ).toList().blockingGet()

            def listResults = CommvaultApiUtility.listSubclients(authConfig, refDataList)
            if(listResults.success) {
                ArrayList<Map> cloudItems = listResults.subclients.findAll { !it.name.endsWith("-cvvm") }
                Observable<BackupJobIdentityProjection> existingItems = morpheusContext.async.backupJob.listIdentityProjections(backupProviderModel)
                SyncTask<BackupJobIdentityProjection, ArrayList<Map>, BackupJob> syncTask = new SyncTask<>(existingItems, cloudItems)

                syncTask.addMatchFunction { BackupJobIdentityProjection domainObject, Map cloudItem ->
                    domainObject.externalId == cloudItem.externalId
                }.onDelete { List<BackupJobIdentityProjection> removeItems ->
                    deleteSubclients(removeItems)
                }.onUpdate { List<SyncTask.UpdateItem<BackupJob, Map>> updateItems ->
                    updateMatchedSubclients(updateItems)
                }.onAdd { itemsToAdd ->
                    addMissingSubclients(itemsToAdd)
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
        log.debug "deleteSubclients: ${removeItems}"
        morpheusContext.async.resourcePermission.remove(removeItems).subscribe().dispose()
        morpheusContext.async.backupJob.remove(removeItems).subscribe().dispose()
    }

    private updateMatchedSubclients(List<SyncTask.UpdateItem<BackupJob, Map>> updateItems) {
        log.debug "updateMatchedSubclients: ${updateItems}"
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

            if (doSave == true) {
                log.debug "updating subclients!! ${existingItem.name}"
                morpheusContext.async.backupJob.save(existingItem).blockingGet()
            }
        }
    }

    private addMissingSubclients(itemsToAdd) {
        log.debug "addMissingSubclients: ${itemsToAdd}"

        def adds = []
        def objCategory = "commvault.job.${backupProviderModel.id}"

        for(cloudItem in itemsToAdd) {
            def name = cloudItem.name == 'default' ? "${cloudItem.name} [${cloudItem.clientName}]" : cloudItem.name
            def addConfig = [
                    account         : backupProviderModel.account,
                    backupProvider  : backupProviderModel,
                    code            : objCategory + '.' + cloudItem.externalId,
                    category        : objCategory,
                    name            : name,
                    externalId      : cloudItem.externalId,
                    source          : 'commvault',
                    enabled         : cloudItem.backupEnabled,
                    internalId      : cloudItem.internalId,
                    enabled         : true
            ]

            def add = new BackupJob(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        if(adds.size() > 0) {
            log.debug "adding subclients: ${adds}"
            BulkCreateResult<BackupJob> result =  morpheusContext.async.backupJob.bulkCreate(adds).blockingGet()
            if(!result.success) {
                log.error "Error adding subclients: ${result.errorCode} - ${result.msg}"
            }
        }
    }
}
