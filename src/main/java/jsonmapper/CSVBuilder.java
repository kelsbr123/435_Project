package jsonmapper;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CSVBuilder {

    public static String buildRows(String user, ArrayList<String> purchaseHistory){

        StringBuilder csv = new StringBuilder();
        StringBuilder row = new StringBuilder();


        for(String k: purchaseHistory){
            String[] tuple = k.split(":");
            row.append("\n").append(user).append(",");
            for(String j : purchaseHistory){
                if(!j.equals(k)) row.append(j.split(":")[0]).append(" ");
            }
            row.append(",").append(tuple[0]).append(",").append(tuple[1]);
            csv.append(row);
        }
        return csv.toString().strip();

    }
}
