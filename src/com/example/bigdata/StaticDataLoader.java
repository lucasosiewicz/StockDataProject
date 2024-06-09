package com.example.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class StaticDataLoader {

    public static Map<String, String> createHashMap(String filePath) {
        Map<String, String> dictionary = new HashMap<>();
        try {
            BufferedReader file = new BufferedReader(new FileReader(filePath));
            String line = file.readLine();
            while ((line = file.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length > 2) {
                    dictionary.put(fields[1], fields[2]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dictionary;
    }
}
