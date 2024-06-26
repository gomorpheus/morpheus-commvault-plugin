package com.morpheusdata.commvault.datasets

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.Workload
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class CommvaultStoragePoliciesDatasetProvider extends AbstractDatasetProvider<ReferenceData, Long> {

    public static final providerName = 'Commvault Storage Policies Dataset Provider'
    public static final providerNamespace = 'commvault'
    public static final providerKey = 'commvaultStoragePolicies'
    public static final providerDescription = 'A set of data from commvault storage policies'

    CommvaultStoragePoliciesDatasetProvider(Plugin plugin, MorpheusContext morpheus) {
        this.plugin = plugin
        this.morpheusContext = morpheus
    }

    @Override
    DatasetInfo getInfo() {
        new DatasetInfo(
                name: providerName,
                namespace: providerNamespace,
                key: providerKey,
                description: providerDescription
        )
    }

    @Override
    Class<ReferenceData> getItemType() {
        return ReferenceData.class
    }

    @Override
    Observable list(DatasetQuery datasetQuery) {
        log.debug("list: ${datasetQuery.parameters}")
        def tmpAccount = datasetQuery.user.account
        Long cloudId = datasetQuery.get("zoneId")?.toLong()
        Long containerId = datasetQuery.get("containerId")?.toLong()

        Cloud cloud = null
        BackupProvider backupProvider = null

        if (containerId && !cloudId) {
            Workload workload = morpheusContext.services.workload.find(new DataQuery().withFilter("account", tmpAccount)
                    .withFilter("containerId", containerId))
            cloud = workload?.server?.cloud
        }

        if (!cloud && cloudId) {
            cloud = morpheusContext.services.cloud.get(cloudId)
        }

        if (cloud?.backupProvider) {
            backupProvider = morpheusContext.services.backupProvider.find(new DataQuery().withFilter("account", cloud.account)
                    .withFilter("id", cloud.backupProvider.id))
        }

        if (backupProvider) {
            return morpheusContext.services.referenceData.list(new DataQuery()
                    .withFilter("category", "${backupProvider?.type?.code}.backup.storagePolicy.${backupProvider?.id}"))
        }

        return Observable.empty()
    }

    @Override
    Observable<Map> listOptions(DatasetQuery datasetQuery) {
        log.debug("listOptions: ${datasetQuery.parameters}")

        def results = list(datasetQuery).toList().blockingGet().collect { refData ->
            [name: refData.name, id: refData.id, value: refData.id]
        }

        def storagePolicies = results.isEmpty() ? [[name: "No Storage Policies setup in Commvault", id: '']] : results

        return Observable.fromIterable(storagePolicies)
    }


    @Override
    ReferenceData fetchItem(Object value) {
        def rtn = null
        if(value instanceof Long) {
            rtn = item((Long)value)
        } else if(value instanceof CharSequence) {
            def longValue = value.isNumber() ? value.toLong() : null
            if(longValue) {
                rtn = item(longValue)
            }
        }
        return rtn
    }

    @Override
    ReferenceData item(Long value) {
        return morpheus.services.referenceData.get(value)
    }

    @Override
    String itemName(ReferenceData item) {
        return item.name
    }

    @Override
    Long itemValue(ReferenceData item) {
        return item.id
    }

    @Override
    boolean isPlugin() {
        return true
    }
}
