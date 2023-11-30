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
        ArrayList<String> temp = new ArrayList<>();


        for(String k: purchaseHistory){
            for(String j : purchaseHistory){
                if(!j.equals(k)) temp.add(j);
            }
            String[] tuple = k.split(":");
            row.append("\n" + user + ",");
            for(int i  = temp.size()-4; i< temp.size(); i++){
                row.append(temp.get(i).split(":")[0] + ",");
            }
            row.append(tuple[0] + "," + tuple[1]);
            csv.append(row);
            temp.clear();
        }
        return csv.toString().strip();

    }
}
