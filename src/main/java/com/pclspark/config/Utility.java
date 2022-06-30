package com.pclspark.config;

import java.util.HashMap;
import java.util.Map;

public class Utility {
  public static Map<String, String> getArgumentMap(String[] args) {
    Map<String, String> hashMap = new HashMap<>();

    if (args != null && args.length > 0) {
      for (String arg: args) {
        String [] keyValue = arg.split("=");
        hashMap.put(keyValue[0], keyValue[1]);
      }
    }

    return hashMap;
  }
}
