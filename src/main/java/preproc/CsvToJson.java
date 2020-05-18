package preproc;

import java.io.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.csv.*;

public class CsvToJson {
    public static List<Map<String, ?>> Convert(String inputcsv) throws IOException {

        File input = new File(inputcsv);
        CsvSchema csv = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<String, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(input);
        List<Map<String, ?>> list = mappingIterator.readAll();
        return list;
    }
}

