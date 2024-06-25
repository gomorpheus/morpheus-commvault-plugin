package com.morpheusdata.commvault.utils

/**
 * @author rahul.ray
 */
class CommvaultComputeUtility {

    static String getvsInstanceType(String clientInstanceType) {
        def instanceTypeMapping = [
                '0': "none",
                '1': "vmware",
                '2': "hyperv",
                '3': "xen",
                '4': "amazon",
                '5': "azure",
                '6': "Red Hat Enterprise Virtualization (RHEV)",
                '7': "Microsoft Azure Resource Manager",
                '9': "nutanix",
                '10': "oraclevm",
                '11': "docker",
                '12': "openstack",
                '13': "oracle",
                '14': "huawei",
                '16': "google",
                '17': "Microsoft Azure Stack",
                '19': "oraclecloud"
        ]
        return instanceTypeMapping[clientInstanceType]
    }
}
