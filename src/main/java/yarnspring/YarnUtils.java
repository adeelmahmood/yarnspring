package yarnspring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnUtils {

	private static final Logger log = LoggerFactory.getLogger(YarnUtils.class);

	public static void printClusterStats(YarnClient yarnClient, String queue) throws YarnException, IOException {
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		log.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		log.info("Got Cluster node info from ASM");
		for (NodeReport node : clusterNodeReports) {
			log.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress"
					+ node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers"
					+ node.getNumContainers());
		}

		QueueInfo queueInfo = yarnClient.getQueueInfo(queue);
		log.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
				+ queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
				+ ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount="
				+ queueInfo.getChildQueues().size());

		List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
			for (QueueACL userAcl : aclInfo.getUserAcls()) {
				log.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
						+ userAcl.name());
			}
		}
	}

	public static String printEnvAndClasspath() {
		StringBuffer buffer = new StringBuffer();
		log.info("printing environment and classpath");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			log.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
			buffer.append("System env: key=" + env.getKey() + ", val=" + env.getValue() + "\n");
		}

		BufferedReader buf = null;
		try {
			String pwd = Shell.WINDOWS ? Shell.execCommand("echo", " %cd%") : Shell.execCommand("pwd");
			buf = new BufferedReader(new StringReader(pwd));
			String line = "";
			while ((line = buf.readLine()) != null) {
				log.info("PWD: " + line);
				System.out.println("PWD: " + line);
				buffer.append("PWD: " + line + "\n");
			}
		} catch (IOException e) {
			log.error("error in printing env and classpath", e);
		}
		try {
			String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") : Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				log.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
				buffer.append("System CWD content: " + line + "\n");
			}
		} catch (IOException e) {
			log.error("error in printing env and classpath", e);
		}
		return buffer.toString();
	}

	public static List<String> createCommand(Vector<CharSequence> vargs) {
		// Get final commmand
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}
		log.info("Created command => " + command.toString());
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		return commands;
	}

	public static void checkApplicationEnvRequiredParams(Map<String, String> envs) {
		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}
	}
}