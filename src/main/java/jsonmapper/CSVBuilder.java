package jsonmapper;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CSVBuilder {

    public static String buildRows(String user, ArrayList<String> purchaseHistory, boolean encode){

        StringBuilder csv = new StringBuilder();


        for(String k: purchaseHistory){
            if(encode) csv.append(buildEncodedRow(user,purchaseHistory,k));

            else csv.append(buildRow(user,purchaseHistory,k));
        }
        return csv.toString().strip();

    }

    private static String buildEncodedRow(String user, ArrayList<String> history, String k) {
        StringBuilder row = new StringBuilder();
        ArrayList<String> temp = new ArrayList<>();
        for(String j : history){
                if(!j.equals(k)) temp.add(j);
            }
            String[] tuple = k.split(":");
            row.append("\n" + user + ",");
            for(int i  = temp.size()-4; i< temp.size(); i++){
                row.append(temp.get(i).split(":")[0] + ",");
            }
            row.append(tuple[0] + "," + tuple[1]);
            temp.clear();
            return row.toString();
    }

    private static String buildRow(String user, ArrayList<String> history, String k){
        StringBuilder row = new StringBuilder();
        String[] tuple = k.split(":");
        row.append("\n").append(user).append(",");
        for(String j : history){
            if(!j.equals(k)) row.append(j.split(":")[0]).append(" ");
        }

        row.append(",").append(tuple[0]).append(",").append(tuple[1]);

        return row.toString();
    }
}
