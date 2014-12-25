package yarnspring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Runner implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(Runner.class);
	
	public static void main(String[] args) {
		SpringApplication.run(Runner.class, args);
	}
	
	@Override
	public void run(String... arg0) throws Exception {
		log.info("+++++++ RUNNER STARTED +++++++++");
	}
}
