package home.my.sparkloganalyzer.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class LogService implements Serializable {
    private final JavaSparkContext sparkContext;

    @Autowired
    public LogService(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public List<String> analyze(String pathToFile) {
        List<String> data = sparkContext.textFile(pathToFile).collect();

        JavaRDD<String> lines = sparkContext.textFile(pathToFile);
        return lines.map(s -> s.replaceAll("(\\[\\d+\\])", ""))
                .filter((Function<String, Boolean>) s -> s.contains("Field is not present in the schema"))
                .distinct()
                .collect();
    }
}
