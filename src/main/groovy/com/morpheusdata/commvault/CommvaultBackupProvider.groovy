package com.morpheusdata.commvault

import com.morpheusdata.commvault.util.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusAccountCredentialService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultBackupProvider extends AbstractBackupProvider {

	BackupJobProvider backupJobProvider;
	MorpheusAccountCredentialService credentialService;

	static apiBasePath = '/SearchSvc/CVWebService.svc'

	CommvaultBackupProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)

		CommvaultBackupTypeProvider backupTypeProvider = new CommvaultBackupTypeProvider(plugin, morpheus)
		plugin.registerProvider(backupTypeProvider)
		addScopedProvider(backupTypeProvider, "vmware", null)
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'commvault-backup'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Commvault Backup Provider'
	}
	
	/**
	 * Returns the integration logo for display when a user needs to view or add this integration
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:"icon.svg", darkPath: "icon-dark.svg")
	}

	/**
	 * Sets the enabled state of the provider for consumer use.
	 */
	@Override
	public Boolean getEnabled() { return true; }

	/**
	 * The backup provider is creatable by the end user. This could be false for providers that may be
	 * forced by specific CloudProvider plugins, for example.
	 */
	@Override
	public Boolean getCreatable() { return true; }
	
	/**
	 * The backup provider supports restoring to a new workload.
	 */
	@Override
	public Boolean getRestoreNewEnabled() { return true; }

	/**
	 * The backup provider supports backups. For example, a backup provider may be intended for disaster recovery failover
	 * only and may not directly support backups.
	 */
	@Override
	public Boolean getHasBackups() { return true; }

	/**
	 * The backup provider supports creating new jobs.
	 */
	@Override
	public Boolean getHasCreateJob() { return true; }

	/**
	 * The backup provider supports cloning a job from an existing job.
	 */
	@Override
	public Boolean getHasCloneJob() { return true; }

	/**
	 * The backup provider can add a workload backup to an existing job.
	 */
	@Override
	public Boolean getHasAddToJob() { return true; }

	/**
	 * The backup provider supports backups outside an encapsulating job.
	 */
	@Override
	public Boolean getHasOptionalJob() { return true; }

	/**
	 * The backup provider supports scheduled backups. This is primarily used for display of hte schedules and providing
	 * options during the backup configuration steps.
	 */
	@Override
	public Boolean getHasSchedule() { return true; }

	/**
	 * The backup provider supports running multiple workload backups within an encapsulating job.
	 */
	@Override
	public Boolean getHasJobs() { return true; }

	/**
	 * The backup provider supports retention counts for maintaining the desired number of backups.
	 */
	@Override
	public Boolean getHasRetentionCount() { return true; }

	/**
	 * Get the list of option types for the backup provider. The option types are used for creating and updating an
	 * instance of the backup provider.
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes
	}

	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating and updating
	 * replication groups.
	 */
	@Override
	Collection<OptionType> getReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}
	
	/**
	 * Get the list of replication option types for the backup provider. The option types are used for creating and updating
	 * replications.
	 */
	@Override
	Collection<OptionType> getReplicationOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the list of backup job option types for the backup provider. The option types are used for creating and updating
	 * backup jobs.
	 */
	@Override
	Collection<OptionType> getBackupJobOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of backup option types for the backup provider. The option types are used for creating and updating
	 * backups.
	 */
	@Override
	Collection<OptionType> getBackupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}
	
	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating
	 * replications on an instance during provisioning.
	 */
	@Override
	Collection<OptionType> getInstanceReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the {@link BackupJobProvider} responsible for all backup job operations in this backup provider
	 * The {@link DefaultBackupJobProvider} can be used if the provider would like morpheus to handle all job operations.
	 * @return the {@link BackupJobProvider} for this backup provider
	 */
	@Override
	BackupJobProvider getBackupJobProvider() {
		// The default backup job provider allows morpheus to handle the
		// scheduling and execution of the jobs. Replace the default job provider
		// if jobs are to be managed on the external backup system.
		if(!this.backupJobProvider) {
			this.backupJobProvider = new DefaultBackupJobProvider(getPlugin(), morpheus);
		}
		return this.backupJobProvider
	}

	/**
	 * Apply provider specific configurations to a {@link com.morpheusdata.model.BackupProvider}. The standard configurations are handled by the core system.
	 * @param backupProviderModel backup provider to configure
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackupProvider(BackupProviderModel backupProviderModel, Map config, Map opts) {
		return ServiceResponse.success(backupProviderModel)
	}

	/**
	 * Validate the configuration of the {@link com.morpheusdata.model.BackupProvider}. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupProviderModel backup provider to validate
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * validation and will halt the backup provider creation process.
	 */
//	@Override
//	ServiceResponse validateBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
//		def rtn = ServiceResponse.success(backupProviderModel)
//		return rtn
//	}
	@Override
	ServiceResponse validateBackupProvider(BackupProvider backupProvider, Map opts) {
//		log.debug "validateBackupProvider: ${backupProvider}"
		log.info("RAZI :: validateBackupProvider : backupProvider: ${backupProvider} :: opts: ${opts}")
		def rtn = [success:false, errors:[:]]
		try {
			def apiOpts = [:]
			//validate input fields
			rtn.data = backupProvider
			//credentials
			def credential = credentialService.loadCredentialConfig(opts.credential, [username: backupProvider.username, password: backupProvider.password])
			log.info("RAZI :: credential: ${credential}")
			log.info("RAZI :: credential.data?.username: ${credential.data?.username}")
			if(!credential.data?.username) {
				rtn.errors.username = 'Enter a username'
				rtn.msg = 'Missing required parameter'
			}
			log.info("RAZI :: credential.data?.password: ${credential.data?.password}")
			if(!credential.data?.password) {
				rtn.errors.password = 'Enter a password'
				rtn.msg = 'Missing required parameter'
			}
			if(!rtn.errors) {
				backupProvider.credentialData = credential.data
				backupProvider.credentialLoaded = true
				log.info("RAZI :: backupProvider: ${backupProvider}, apiOpts: ${apiOpts}")
				def testResults = testConnection(backupProvider, apiOpts)
				log.info("RAZI :: testResults: ${testResults}")
//				log.debug("api test results: {}", testResults)
				if (testResults.success == true)
					rtn.success = true
				else if (testResults.invalidLogin == true)
					rtn.msg = testResults.msg ?: 'unauthorized - invalid credentials'
				else if (testResults.found == false)
					rtn.msg = testResults.msg ?: 'commvault not found - invalid host'
				else
					rtn.msg = testResults.msg ?: 'unable to connect to commvault'
			}
		} catch(e) {
			log.error("error validating commvault configuration: ${e}", e)
			rtn.msg = 'unknown error connecting to commvault'
			rtn.success = false
		}
		log.info("RAZI :: RTN : validateBackupProvider: ${rtn}")
		return ServiceResponse.create(rtn)
	}

	def testConnection(BackupProvider backupProvider, Map opts) {
		def rtn = [success:false, invalidLogin:false, found:true]
		opts.authConfig = opts.authConfig ?: getAuthConfig(backupProvider)
		log.info("RAZI :: opts.authConfig: ${opts.authConfig}")
		def tokenResults = loginSession(opts.authConfig.apiUrl, opts.authConfig.username, opts.authConfig.password)
		log.info("RAZI :: tokenResults: ${tokenResults}")
		if(tokenResults.success == true) {
			rtn.success = true
			def token = tokenResults.tokenf
			def sessionId = tokenResults.sessionId
			log.info("RAZI :: calling logoutSession")
			logoutSession(opts.authConfig.apiUrl, token)
			log.info("RAZI :: logoutSession COMPLETED")
		} else {
			log.info("RAZI :: calling to error code")
			if(tokenResults?.errorCode == '404' || tokenResults?.errorCode == 404)
				rtn.found = false
			if(tokenResults?.errorCode == '401' || tokenResults?.errorCode == 401)
				rtn.invalidLogin = true
			rtn.msg = tokenResults.msg
			rtn.errorCode = tokenResults.errorCode
			log.info("RAZI :: calling to error code COMPLETED")
		}
		log.info("RAZI :: RTN : testConnection: ${rtn}")
		return rtn
	}

	def getAuthConfig(BackupProvider backupProvider) {
		//credentials
		credentialService.loadCredentials(backupProvider)
		def rtn = [
				apiUrl:getApiUrl(backupProvider),
				username:backupProvider.credentialData?.username ?: backupProvider.username,
				password:backupProvider.credentialData?.password ?: backupProvider.password,
				basePath:apiBasePath
		]
		log.info("RAZI :: RTN : getAuthConfig: ${rtn}")
		return rtn
	}

	def getApiUrl(BackupProvider backupProvider) {
		def scheme = backupProvider.host.contains("http") ? "" : "http://"
		def apiUrl = "${scheme}${backupProvider.host}:${backupProvider.port}"

		return apiUrl
	}

	def loginSession(String apiUrl, String username, String password) {
		def rtn = [success: false]
		def token
		def response = CommvaultBackupUtility.getToken(apiUrl, username, password)
		if(response.success) {
			rtn.success = true
			rtn.token = response.token
		} else {
			rtn.success = false
			rtn.msg = response.msg
			rtn.errorCode = response.errorCode
		}
		return rtn
	}

	def logoutSession(String apiUrl, String token) {
		if(token) {
			CommvaultBackupUtility.logout(apiUrl, token)
			log.info("RAZI :: calling logoutSession SUCCESS")
		}
	}

	/**
	 * Delete the backup provider. Typically used to clean up any provider specific data that will not be cleaned
	 * up by the default remove in the core system.
	 * @param backupProviderModel the backup provider being removed
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * delete and will halt the process.
	 */
	@Override
	ServiceResponse deleteBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * The main refresh method called periodically by Morpheus to sync any necessary objects from the integration.
	 * This can call sub services for better organization. It is recommended that {@link com.morpheusdata.core.util.SyncTask} is used.
	 * @param backupProvider the current instance of the backupProvider being refreshed
	 * @return the success state of the refresh
	 */
	@Override
	ServiceResponse refresh(BackupProviderModel backupProviderModel) {
		return ServiceResponse.success()
	}
}
