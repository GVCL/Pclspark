package com.pclspark;

import com.pclspark.config.Utility;
import com.pclspark.worker.example.SparkCassandra;

public class Main
{
    public static  void main(String args[])
    {
        SparkCassandra.process(Utility.getArgumentMap(args));
    }
}