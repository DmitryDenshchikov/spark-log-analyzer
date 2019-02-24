package home.my.sparkloganalyzer;

import home.my.sparkloganalyzer.services.LogService;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class SparkLogAnalyzerApplication implements CommandLineRunner {
	@Autowired
	LogService logService;

	public static void main(String[] args) {
		SpringApplication.run(SparkLogAnalyzerApplication.class, args);
		System.out.println();
	}

	@Bean
	public JavaSparkContext sc() {
		SparkConf conf = new SparkConf().setAppName("log analyzer").setMaster("local[*]");
		return new JavaSparkContext(conf);
	}

	@Override
	public void run(String... args) throws Exception {
		File folder = new File("C:\\Users\\user\\Desktop\\File");
		List<String> allDifferences = new ArrayList<>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				System.out.println(fileEntry.getName() + " is folder");
			} else {
				allDifferences.addAll(logService.analyze(fileEntry.getAbsolutePath()));
			}
		}
		System.out.println("\r\n" + String.join("\r\n", allDifferences.stream().distinct().collect(Collectors.toList())));
	}
}
