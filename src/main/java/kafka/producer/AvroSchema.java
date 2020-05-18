package kafka.producer;

import org.apache.avro.generic.GenericRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class AvroSchema {
    public String schemaStringGeo="{" +
            "  \"name\": \"MyClass\"," +
            "  \"type\": \"record\"," +
            "  \"namespace\": \"com.acme.avro\"," +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"longitude\"," +
            "      \"type\": \"double\"" +
            "    }," +
            "    {" +
            "      \"name\": \"lattitude\"," +
            "      \"type\": \"double\"" +
            "    }" +
            "  ]" +
            "}";

    public  String schemaString = "{" +
            "  \"name\": \"MyClass\"," +
            "  \"type\": \"record\"," +
            "  \"namespace\": \"com.acme.avro\"," +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"DRNumber\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"DateReported\"," +
            "      \"type\": \"int\"," +
            "      \"logicalType\": \"date\"" +
            "    }," +
            "    {" +
            "      \"name\": \"DateOccurred\"," +
            "      \"type\": \"int\"," +
            "      \"logicalType\": \"date\"" +
            "    }," +
            "    {" +
            "      \"name\": \"TimeOccurred\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"AreaID\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"AreaName\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"ReportingDistrict\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"CrimeCode\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"CrimeCodeDescription\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"MOCodes\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"VictimAge\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"VictimSex\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"VictimDescent\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"PremiseCode\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"PremiseDescription\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"CrossStreet\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"Location\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"ZipCodes\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"CensusTracts\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"PrecinctBoundaries\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"LASpecificPlans\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"CouncilDistricts\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"NeighborhoodCouncilsCertified\"," +
            "      \"type\": \"string\"" +
            "    }" +
            "  ]" +
            "}";
    public GenericRecord SetupTrafficScheme(GenericRecord customer, Map<?,?> element){

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
        Date date = new Date();
        Date date2 = new Date();
        try {
            date = formatter.parse(element.get("Date Occurred").toString());
            date2 = formatter.parse(element.get("Date Reported").toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        customer.put("DRNumber",element.get("DR Number").toString());
        customer.put("DateReported",date2.getTime());
        customer.put("DateOccurred", date.getTime());
        customer.put("TimeOccurred",element.get("Time Occurred").toString());
        customer.put("AreaID",element.get("Area ID").toString());
        customer.put("AreaName",element.get("Area Name").toString());
        customer.put("ReportingDistrict",element.get("Reporting District").toString());
        customer.put("CrimeCode",element.get("Crime Code").toString());
        customer.put("CrimeCodeDescription",element.get("Crime Code Description").toString());
        customer.put("MOCodes",element.get("MO Codes").toString());
        customer.put("VictimAge",element.get("Victim Age").toString());
        customer.put("VictimSex",element.get("Victim Sex").toString());
        customer.put("VictimDescent",element.get("Victim Descent").toString());
        customer.put("PremiseCode",element.get("Premise Code").toString());
        customer.put("PremiseDescription",element.get("Premise Description").toString());
        customer.put("CrossStreet",element.get("Cross Street").toString());
        customer.put("Location",element.get("Location").toString().replaceAll("'","\""));
        customer.put("ZipCodes",element.get("Zip Codes").toString());
        customer.put("CensusTracts",element.get("Census Tracts").toString());
        customer.put("PrecinctBoundaries",element.get("Precinct Boundaries").toString());
        customer.put("LASpecificPlans",element.get("LA Specific Plans").toString());
        customer.put("CouncilDistricts",element.get("Council Districts").toString());
        customer.put("NeighborhoodCouncilsCertified",element.get("Neighborhood Councils (Certified)").toString());

        return customer;
    }
}
