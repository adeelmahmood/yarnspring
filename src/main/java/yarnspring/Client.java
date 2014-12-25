package yarnspring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

	private static final Logger log = LoggerFactory.getLogger(Client.class);

	private Configuration conf;
	private YarnClient yarnClient;
	private FileSystem fs;

	private String appName;

	private Options opts;

	private int priority;
	private String queue;
	private int amMem;
	private int amVCores;
	private String amJar;
	private String containerJar;
	private int numContainers;
	private int containerMem;
	private int containerVCores;
	private int containerPriority;
	private boolean keepContainersAcrossRestart;
	private boolean debug;

	private final long startTime = System.currentTimeMillis();
	private long timeout;

	public static void main(String[] args) throws Exception {
		boolean result = false;
		try {
			Client client = new Client();
			try {
				// initialize client
				boolean run = client.init(args);
				if (!run) {
					System.exit(0);
				}
				// run client
				result = client.run();
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				client.printHelp();
				System.exit(-1);
			}
		} catch (Throwable t) {
			log.error("error running client", t);
			System.exit(1);
		}
		// check result status
		if (result) {
			log.info("application completed successfully");
			System.exit(0);
		}
		log.error("application failed");
		System.exit(2);
	}

	public Client() throws IOException {
		conf = new YarnConfiguration();
		fs = FileSystem.get(conf);
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);

		// specify command line params
		opts = new Options();
		opts.addOption("name", true, "Application name");
		opts.addOption("prority", true, "Application priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which application is submitted");
		opts.addOption("timeout", true, "Application timeout in ms");
		opts.addOption("master_mem", true, "Memory to request for application master");
		opts.addOption("master_vcores", true, "Virtual cores to request for application master");
		opts.addOption("jar", true, "Application master jar");
		opts.addOption("container_jar", true, "Containers jar");
		opts.addOption("num_containers", true, "Number of containers to request to run application");
		opts.addOption("container_mem", true, "Memory to request for containers");
		opts.addOption("container_vcores", true, "Virtual cores to request containers");
		opts.addOption("container_priority", true, "Container's priority. Default 0");
		opts.addOption("keep_containers_across_restart", false, "Keep containers across application restarts");
		opts.addOption("debug", false, "Display debug information in logs");
		opts.addOption("help", false, "Display help");
	}

	public boolean init(String[] args) throws ParseException {
		// parse command line params
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (args.length == 0) {
			throw new IllegalArgumentException("No arguments specified");
		}

		if (cliParser.hasOption("help")) {
			printHelp();
			return false;
		}

		// set properties from command line params
		appName = cliParser.getOptionValue("name");
		priority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		queue = cliParser.getOptionValue("queue", "default");
		timeout = Long.parseLong(cliParser.getOptionValue("timeout", "600000"));

		amMem = Integer.parseInt(cliParser.getOptionValue("master_mem", "10"));
		amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
		if (amMem < 0) {
			throw new IllegalArgumentException("Invalid value " + amMem + " specified for master_mem");
		}
		if (amVCores < 0) {
			throw new IllegalArgumentException("Invalid value " + amVCores + " specified for master_vcores");
		}

		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("Application master jar not specified");
		}
		amJar = cliParser.getOptionValue("jar");
		containerJar = cliParser.getOptionValue("container_jar", amJar);
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		containerMem = Integer.parseInt(cliParser.getOptionValue("container_mem", "10"));
		containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		containerPriority = Integer.parseInt(cliParser.getOptionValue("container_priority", "0"));
		if (numContainers < 0 || containerMem < 0 || containerVCores < 0) {
			throw new IllegalArgumentException(
					"Invalid values specified for num_container, container_mem or container_vcores as ["
							+ numContainers + "," + containerMem + "," + containerVCores + "]");
		}

		keepContainersAcrossRestart = cliParser.hasOption("keep_containers_across_restart");
		debug = cliParser.hasOption("debug");

		log.info("client initialized");
		return true;
	}

	private void printHelp() {
		new HelpFormatter().printHelp("Client", opts);
	}

	public boolean run() throws Exception {
		log.info("running client");
		yarnClient.start();
		if (debug) {
			YarnUtils.printClusterStats(yarnClient, queue);
		}

		// create new yarn application
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

		// get container capabilities and adjust our requirements
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		log.info("maximum memory allowed => " + maxMem);
		if (amMem > maxMem) {
			log.info("request application master memory " + amMem + " is more than max memory " + maxMem
					+ ", adjusting");
			amMem = maxMem;
		}

		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		log.info("maximum virtual cores allowed => " + maxVCores);
		if (amVCores > maxVCores) {
			log.info("requested virtual cores " + amVCores + " is more than max virtual cores " + maxVCores
					+ ", adjusting");
			amVCores = maxVCores;
		}

		// create application context
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		// set application general attributes
		appContext.setApplicationName(appName);
		appContext.setKeepContainersAcrossApplicationAttempts(keepContainersAcrossRestart);

		// create application master container
		ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);

		// set local resource for application master
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		// add application master jar to resource
		ResourceUtils.addLocalResource(fs, amJar, "AppMaster.jar", appName, appId.toString(), localResources);

		// specify local resource on container
		appContainer.setLocalResources(localResources);

		// create environment
		Map<String, String> env = new HashMap<String, String>();

		// if container jar is different than am jar make it available in hdfs
		if(!containerJar.equals(amJar)){
			String suffix = appName + Path.SEPARATOR + appId + Path.SEPARATOR + "Containers.jar";
			Path dest = new Path(fs.getHomeDirectory(), suffix);
			log.info("uploading container jar [" + containerJar + "] to hdfs [" + dest.toString() + "]");
			fs.copyFromLocalFile(new Path(containerJar), dest);
			//pass the path to AM
			env.put("CONTAINER_JAR", dest.toString());
		}
		
		// set classpath
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(
				ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());

		// specify environment on container
		appContainer.setEnvironment(env);
		if (debug) {
			log.info("Environment set as => " + env.toString());
		}

		// create a the command to execute application master
		Vector<CharSequence> vargs = new Vector<CharSequence>();
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + amMem + "m");
		// set classname
		vargs.add("yarnspring.ApplicationMaster");
		// pass container params
		vargs.add("--num_containers " + String.valueOf(numContainers));
		vargs.add("--container_mem " + String.valueOf(containerMem));
		vargs.add("--container_vcores " + String.valueOf(containerVCores));
		vargs.add("--container_priority " + String.valueOf(containerPriority));
		if (debug) {
			vargs.add("--debug");
		}
		// logs
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		// set command on container to run application master
		appContainer.setCommands(YarnUtils.createCommand(vargs));

		// specify container requirements
		Resource resource = Records.newRecord(Resource.class);
		resource.setMemory(amMem);
		resource.setVirtualCores(amVCores);
		appContext.setResource(resource);
		log.info("container capability set as " + amMem + "m	 memory and and " + amVCores + " virtual cores");

		// Setup security tokens
		if (UserGroupInformation.isSecurityEnabled()) {
			Credentials credentials = new Credentials();
			String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
			}

			// For now, only getting tokens for the default file-system.
			final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
			if (tokens != null) {
				for (Token<?> token : tokens) {
					log.info("Got dt for " + fs.getUri() + "; " + token);
				}
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			appContainer.setTokens(fsTokens);
		}

		// container setup complete, set on context
		appContext.setAMContainerSpec(appContainer);

		// set priroity
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(priority);
		appContext.setPriority(pri);
		log.info("using priority " + pri);

		// set queue
		appContext.setQueue(queue);
		log.info("using queue " + queue);

		// submit application
		yarnClient.submitApplication(appContext);

		// continue to monitor application
		return monitor(appId);
	}

	private boolean monitor(ApplicationId appId) throws YarnException, IOException {
		while (true) {
			try {
				// check status every second
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}

			// get application status report
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			if (debug) {
				log.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken="
						+ report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics()
						+ ", appMasterHost=" + report.getHost() + ", appQueue=" + report.getQueue()
						+ ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
						+ ", yarnAppState=" + report.getYarnApplicationState().toString() + ", distributedFinalState="
						+ report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
						+ ", appUser=" + report.getUser());
			}

			// get application statuses
			YarnApplicationState yarnStatus = report.getYarnApplicationState();
			FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
			if (FinalApplicationStatus.SUCCEEDED == finalStatus) {
				if (YarnApplicationState.FINISHED == yarnStatus) {
					log.info("application completed successfully, stopping monitoring");
					return true;
				} else {
					log.info("application did not completed successfully. yarn status = " + yarnStatus.toString()
							+ ", final status = " + finalStatus.toString());
					return false;
				}
			} else if (FinalApplicationStatus.KILLED == finalStatus || FinalApplicationStatus.FAILED == finalStatus) {
				log.info("application ended prematurely. yarn status = " + yarnStatus.toString() + ", final status = "
						+ finalStatus.toString());
				return false;
			}

			// see if application needs to be terminated
			if (System.currentTimeMillis() > (startTime + timeout)) {
				log.info("timeout has elapsed, killing application");
				yarnClient.killApplication(appId);
			}
		}
	}
}