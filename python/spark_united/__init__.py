#
# Code borrowed and modified from https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
#
import os
import arcpy
pro_home = arcpy.GetInstallInfo()["InstallDir"]
pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")

#### Local Pro Spark Only -----------------------
import sys
# add spark/py4j libraries from Pro runtime to path for import
pro_lib_dir = os.path.join(pro_home, "Java", "lib")
spark_home = os.path.join(pro_runtime_dir, "spark")
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "pyspark.zip"))
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "py4j-0.10.7-src.zip"))
#### -----------------------------------------------

import subprocess
from typing import Dict

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from spark.java_gateway import launch_gateway

SparkContext._gateway = None


def spark_start(config: Dict = {}) -> SparkSession:
    pro_home = arcpy.GetInstallInfo()["InstallDir"]
    # pro_lib_dir = os.path.join(pro_home, "Java", "lib")
    pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")
    os.environ["HADOOP_HOME"] = os.path.join(pro_runtime_dir, "hadoop")
    #
    # these need to be reset on every run or pyspark will think the Java gateway is still up and running
    os.environ.unsetenv("PYSPARK_GATEWAY_PORT")
    os.environ.unsetenv("PYSPARK_GATEWAY_SECRET")
    SparkContext._jvm = None
    SparkContext._gateway = None

    popen_kwargs = {
        'stdout': subprocess.DEVNULL,  # need to redirect stdout & stderr when running in Pro or JVM fails immediately
        'stderr': subprocess.DEVNULL,
        'shell': True  # keeps the command-line window from showing
    }

    conf = SparkConf()

    #Test if config has spark.master as local or spark.master is not set
    #if so considers ArcGIS Pro Spark as master
    if "spark.master" in config:
        if config["spark.master"].startswith("local"):
            config = _spark_conf_esri(config)
    else:
       config = _spark_conf_esri(config)        
        
    # Add/Update user defined spark configurations.
    for k, v in config.items():
        conf.set(k, v)

    # we have to manage the py4j gateway ourselves so that we can control the JVM process
    gateway = launch_gateway(conf=conf, popen_kwargs=popen_kwargs)
    sc = SparkContext(gateway=gateway)
    spark = SparkSession(sc)
    # Kick start the spark engine.
    spark.sql("select 1").collect()
    return spark


def spark_stop():
    if SparkContext._gateway:
        spark = SparkSession.builder.getOrCreate()
        gateway = spark._sc._gateway
        spark.stop()
        gateway.shutdown()
        gateway.proc.stdin.close()

        # ensure that process and all children are killed
        subprocess.Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(gateway.proc.pid)],
                         shell=True,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        SparkContext._gateway = None


def _spark_conf_esri(config):
    os.environ["JAVA_HOME"] = os.path.join(pro_runtime_dir, "jre")

    # Set to local spark in Pro if not already defined.
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = spark_home
    # Get the active conda env.
    if os.getenv('LOCALAPPDATA'):
        pro_env_txt = os.path.join(os.getenv('LOCALAPPDATA'), 'ESRI', 'conda', 'envs', 'proenv.txt')
        if os.path.exists(pro_env_txt):
            with open(pro_env_txt, "r") as fp:
                python_path = fp.read().strip()
                os.environ["PYSPARK_PYTHON"] = os.path.join(python_path, "python.exe")
        else:
            os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3",
                                                        "python.exe")
    else:
        os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3", "python.exe")

    spark_jars = [os.path.join(pro_lib_dir, "spark-desktop-engine.jar"),
                  os.path.join(pro_lib_dir, "arcobjects.jar")]
    spark_jars = ",".join(spark_jars)

    esri_config = {}
    esri_config["spark.master"] = "local[*]"
    esri_config["spark.ui.enabled"] = False
    esri_config["spark.ui.showConsoleProgress"] = False
    esri_config["spark.sql.execution.arrow.enabled"] = True
    esri_config["spark.sql.catalogImplementation"] = "in-memory"
    esri_config["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
    esri_config["spark.driver.host"] = "localhost"    

    for k,v in esri_config.items():
        if not k in config: 
            config[k] = v
        
    if "spark.jars" in config:
        config["spark.jars"] = config["spark.jars"] + "," + spark_jars
    else:
        config["spark.jars"] = spark_jars
       
    return config
