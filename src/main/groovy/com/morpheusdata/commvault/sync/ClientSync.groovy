package com.morpheusdata.commvault.sync


import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import groovy.util.logging.Slf4j

/**
 * @author rahul.ray
 */

@Slf4j
class ClientSync {

    MorpheusContext morpheus
    BackupProvider backupProvider
    Map authConfigMap

    ClientSync(MorpheusContext morpheus, BackupProvider backupProvider, Map authConfigMap) {
        this.morpheus = morpheus
        this.backupProvider = backupProvider
        this.authConfigMap = authConfigMap
    }

    def execute() {
        log.debug("ClientSync >> execute()")
        try {
            def listResults = CommvaultBackupUtility.listClients(this.authConfigMap)
            log.debug("listResults.success: ${listResults.success}")
            if (listResults.success) {
                def existingItems = morpheus.async.referenceData.listIdentityProjections(
                        new DataQuery().withFilters([new DataFilter('account.id', backupProvider.account.id),
                                                     new DataFilter('category', "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}")])
                )
                SyncTask<ReferenceDataSyncProjection, Map, ReferenceData> syncTask = new SyncTask<>(existingItems, listResults.clients as Collection<Map>)
                syncTask.addMatchFunction { ReferenceDataSyncProjection domainObject, Map cloudItem ->
                    domainObject.externalId.toString() == cloudItem.externalId.toString()
                }.onDelete { removeItems ->
                    log.debug("onDelete removeItems:", removeItems.size())
                    morpheus.services.referenceData.bulkRemove(removeItems)
                }.onAdd { itemsToAdd ->
                    addMissingClients(itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
                    morpheus.async.referenceData.listById(updateItems?.collect { it.existingItem.id }).map { ReferenceData referenceData ->
                        SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map> matchItem = updateItemMap[referenceData.id]
                        return new SyncTask.UpdateItem<ReferenceData, Map>(existingItem: referenceData, masterItem: matchItem.masterItem)
                    }
                }.onUpdate { updateList ->
                    updateMatchedClients(updateList)
                }.start()
            }
        } catch (e) {
            log.error("cacheClients error: ${e}", e)
        }
    }

    private addMissingClients(Collection<Map> addList) {
        log.debug("ClientSync:addMissingClients: addList.size(): ${addList.size()}")
        def clientAdds = []
        try {
            def objCategory = "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}"
            addList?.each { cloudItem ->
                def addConfig = [
                        account   : backupProvider.account,
                        code      : objCategory + '.' + cloudItem.externalId,
                        category  : objCategory,
                        name      : cloudItem.name,
                        keyValue  : cloudItem.externalId,
                        value     : cloudItem.externalId,
                        internalId: cloudItem.internalId,
                        externalId: cloudItem.externalId,
                        type      : 'string'
                ]
                def add = new ReferenceData(addConfig)
                add.setConfigMap(cloudItem)
                clientAdds << add
            }
            if (clientAdds.size() > 0) {
                morpheus.services.referenceData.bulkCreate(clientAdds)
            }
        } catch (e) {
            log.error "Error in adding ClientSync ${e}", e
        }
    }

    private updateMatchedClients(List<SyncTask.UpdateItem<ReferenceData, Map>> updateList) {
        log.debug "updateMatchedClients: ${updateList.size()}"
        def saves = []
        def doSave = false
        try {
            for (updateItem in updateList) {
                doSave = false
                def existingItem = updateItem.existingItem
                def masterItem = updateItem.masterItem
                if (existingItem.name != masterItem.name) {
                    existingItem.name = masterItem.name
                    doSave = true
                }
                existingItem.internalId = masterItem.internalId
                existingItem.externalId = masterItem.externalId
                existingItem.setConfigProperty('vsInstanceType', masterItem.vsInstanceType)
                doSave = true
                if (doSave == true) {
                    saves << existingItem
                }
            }
            if (saves) {
                morpheus.services.referenceData.bulkSave(saves)
            }
        } catch (e) {
            log.error("updateMatchedClients error: ${e}", e)
        }
    }
}
