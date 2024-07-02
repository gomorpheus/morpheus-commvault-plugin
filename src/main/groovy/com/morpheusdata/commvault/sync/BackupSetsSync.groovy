package com.morpheusdata.commvault.sync

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class BackupSetsSync {
    private CommvaultPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel

    BackupSetsSync(BackupProvider backupProviderModel, CommvaultPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("SubclientsSync execute")
            Map authConfig = plugin.getAuthConfig(backupProviderModel)

            def clients = morpheusContext.async.referenceData.list(new DataQuery()
                    .withFilter('account.id', backupProviderModel.account.id)
                    .withFilter('category', "${backupProviderModel.type.code}.backup.backupServer.${backupProviderModel.id}")
            ).toList().blockingGet()
            log.info("RAZI :: clients: ${clients}")

            clients.each { client ->
                def listResults = CommvaultBackupUtility.listBackupSets(authConfig, client)
                log.info("RAZI :: listResults: ${listResults}")
                if(listResults.success) {
                    ArrayList<Map> cloudItems = listResults.backupSets
                    log.info("RAZI :: cloudItems: ${cloudItems}")
                    def objCategory = "${backupProviderModel.type.code}.backup.backupSet.${backupProviderModel.id}.${client.id}"
                    log.info("RAZI :: objCategory: ${objCategory}")
                    Observable<ReferenceDataSyncProjection> existingItems = morpheusContext.async.referenceData.listIdentityProjections(new DataQuery()
                            .withFilter('account.id', backupProviderModel.account.id)
                            .withFilter('category', objCategory)
                    )
                    log.info("RAZI :: existingItems: ${existingItems}")
                    SyncTask<ReferenceDataSyncProjection, ArrayList<Map>, ReferenceData> syncTask = new SyncTask<>(existingItems, cloudItems)

                    syncTask.addMatchFunction { ReferenceDataSyncProjection domainObject, Map cloudItem ->
                        log.info("RAZI :: domainObject.externalId: ${domainObject.externalId}")
                        log.info("RAZI :: cloudItem.externalId: ${cloudItem.externalId}")
                        domainObject.externalId == cloudItem.externalId
                    }.onDelete { List<ReferenceDataSyncProjection> removeItems ->
                        log.info("RAZI :: deleteBackupSets calling")
                        deleteBackupSets(removeItems)
                        log.info("RAZI :: deleteBackupSets calling COMPLETED")
                    }.onUpdate { List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems ->
                        log.info("RAZI :: updateMatchedBackupSets calling")
                        updateMatchedBackupSets(updateItems)
                        log.info("RAZI :: updateMatchedBackupSets calling COMPLETED")
                    }.onAdd { itemsToAdd ->
                        log.info("RAZI :: addMissingBackupSets calling")
                        addMissingBackupSets(itemsToAdd, objCategory, client)
                        log.info("RAZI :: addMissingBackupSets calling COMPLETED")
                    }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItems ->
                        return morpheusContext.async.referenceData.list( new DataQuery(backupProviderModel.account).withFilter("id", 'in', updateItems.collect { it.existingItem.id }))
                    }.start()
                } else {
                    log.error("Error listing backupSets")
                    return Single.just(false).toObservable()
                }
            }
        } catch(Exception ex) {
            log.error("Subclients Sync error: {}", ex, ex)
        }
    }

    private deleteBackupSets(List<ReferenceDataSyncProjection> removeItems) {
//        log.debug "deleteBackupSets: ${removeItems}"
        log.info("RAZI :: removeItems: ${removeItems}")
        morpheusContext.async.referenceData.remove(removeItems).subscribe().dispose()
        log.info("RAZI :: deleteBackupSets call SUCCESS")
    }

    private updateMatchedBackupSets(List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems) {
//        log.debug "updateMatchedBackupSets: ${updateItems}"
        log.info("RAZI :: updateItems: ${updateItems}")
        updateItems.each {updateMap ->

            Boolean doSave = false
            if(updateMap.existingItem.name != updateMap.masterItem.name) {
                updateMap.existingItem.name = updateMap.masterItem.name
                doSave = true
            }

            log.info("RAZI :: doSave: ${doSave}")
            if (doSave == true) {
                log.debug "updating backupSets!! ${updateMap.existingItem.name}"
                morpheusContext.async.referenceData.save(updateMap.existingItem).blockingGet()
                log.info("RAZI :: updateMatchedBackupSets SUCCESS")
            }
        }
    }

    private addMissingBackupSets(itemsToAdd, objCategory, client) {
//        log.debug "addMissingSubclients: ${itemsToAdd}"
        log.info("RAZI :: addMissingBackupSets: ${itemsToAdd}")

        def adds = []
//        def objCategory = "commvault.job.${backupProviderModel.id}"

//        for(cloudItem in itemsToAdd) {
        itemsToAdd.each { Map cloudItem ->
//            def name = cloudItem.name == 'default' ? "${cloudItem.name} [${cloudItem.clientName}]" : cloudItem.name
            def addConfig = [
                    account         : backupProviderModel.account,
//                    backupProvider  : backupProviderModel,
                    code            : objCategory + '.' + cloudItem.externalId,
                    category        : objCategory,
//                    name            : name,
                    name            : cloudItem.name,
                    keyValue        : cloudItem.externalId,
                    value           : cloudItem.externalId,
                    internalId      : cloudItem.internalId,
                    externalId      : cloudItem.externalId,
                    refType         : "ReferenceData",
                    refId           : client.id
            ]

            log.info("RAZI :: addConfig: ${addConfig}")
            def add = new ReferenceData(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        log.info("RAZI :: adds.size(): ${adds.size()}")
        if(adds.size() > 0) {
            log.debug "adding backup sets: ${adds}"
            BulkCreateResult<ReferenceData> result =  morpheusContext.async.referenceData.bulkCreate(adds).blockingGet()
            log.info("RAZI :: addMissingBackupSets : result: ${result}")
            if(!result.success) {
                log.error "Error adding backupSets: ${result.errorCode} - ${result.msg}"
            }
        }
    }
}
