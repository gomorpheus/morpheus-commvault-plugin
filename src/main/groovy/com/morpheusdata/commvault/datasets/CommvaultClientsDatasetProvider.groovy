package com.morpheusdata.commvault.datasets

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class CommvaultClientsDatasetProvider extends AbstractDatasetProvider<Map, Long> {

    public static final providerName = 'Commvault Clients Dataset Provider'
    public static final providerNamespace = 'commvault'
    public static final providerKey = 'commvaultClients'
    public static final providerDescription = 'A collection of key/value pairs' // need to change

    CommvaultClientsDatasetProvider(Plugin plugin, MorpheusContext morpheus) {
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

    Class<Map> getItemType() {
        return Map.class
    }

    Observable<Map> list(DatasetQuery query) {
        return Observable.fromIterable(members)
    }

    Observable<Map> listOptions(DatasetQuery query) {
        return Observable.fromIterable(members)
    }

    Map fetchItem(Object value) {
        def rtn = null
        if (value instanceof Long) {
            rtn = item((Long) value)
        } else if (value instanceof CharSequence) {
            def longValue = value.isNumber() ? value.toLong() : null
            if (longValue) {
                rtn = item(longValue)
            }
        }
        return rtn
    }

    Map item(Long value) {
        def rtn = members.find { it.value == value }
        return rtn
    }

    String itemName(Map item) {
        return item.name
    }

    Long itemValue(Map item) {
        return (Long) item.value
    }

    @Override
    boolean isPlugin() {
        return true
    }
}
