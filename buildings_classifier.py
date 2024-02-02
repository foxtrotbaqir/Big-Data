from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, udf
from pyspark.sql.types import DoubleType

# Define the haversine function
def haversine(lat1, lon1, lat2, lon2):
    R = 6378.1  # radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    deltaPhi = math.radians(lat2 - lat1)
    deltaLambda = math.radians(lon2 - lon1)

    a = math.sin(deltaPhi * deltaPhi / 4.0) + math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# Create a Spark session
spark = SparkSession.builder.appName("BuildingClustering").getOrCreate()

# Load the dataset
file_path = "Enter you dataset address here"
buildings_df = spark.read.parquet(file_path)

# Define constants
start_k = 7
seed_value = 1
area_building_id = "relevant building ID"
area_postal_code = “Enter your area postal code”
max_allowed_cluster_distance = 3.0

# Create a VectorAssembler for latitude and longitude columns
assembler = VectorAssembler(inputCols=["latitude_wgs84", "longitude_wgs84"], outputCol="features")

# Apply the VectorAssembler to the dataset and cache the DataFrame
assembled_df = assembler.transform(buildings_df).cache()

# Define the haversine function as a User-Defined Function (UDF)

@udf(returnType=DoubleType())
def haversine_udf(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2)

# Initial K-Means clustering with k=7
current_cluster = start_k
stop_loop = False
model = None
target_cluster = -1

while not stop_loop:
    # Create K-Means model with increased maxIter
    kmeans = KMeans(k=current_cluster, seed=seed_value, maxIter=50)
    model = kmeans.fit(assembled_df)

    # Find the cluster for targeted building
    sahkotalo_cluster = model.transform(assembled_df)\
        .filter(col("building_id") == area_building_id)\
        .select("prediction")\
        .first()\
        .asDict()["prediction"]

    # Choose all buildings from the current cluster
    current_cluster_df = model.transform(assembled_df)\
        .filter(col("prediction") == target_cluster)\
        .cache()  # Cache the filtered DataFrame

    # Find the center of the current cluster
    cluster_center = model.clusterCenters[sahkotalo_cluster]

    # Calculate the largest distance from a building in the current cluster to the center
    max_distance = current_cluster_df\
        .withColumn("distance", haversine_udf(col("latitude_wgs84"), col("longitude_wgs84"), lit(cluster_center[0]), lit(cluster_center[1])))\
        .select(max("distance"))\
        .first()\
        .asDict()["max(distance)"]

    # If the largest distance is smaller than the threshold, stop the loop
    if max_distance <= max_allowed_cluster_distance:
        stop_loop = True
    else:
        # Reduce cluster count for the next iteration
        current_cluster = max(current_cluster - 2, 2)  # Reduce by a larger step

    # Unpersist cached DataFrames to release memory
    current_cluster_df.unpersist()

# Unpersist the final cached DataFrame
assembled_df.unpersist()

# Final cluster analysis
final_cluster = model.transform(assembled_df).filter(col("prediction") == target_cluster)

# Calculate the total number of buildings in the final cluster
cluster_building_count = final_cluster.count()

# Calculate the number of Area buildings in the final cluster
cluster_area_building_count = final_cluster.filter(col("postal_code") == area_postal_code).count()

# Print the information
print(f"Buildings in the final cluster: {cluster_building_count}")
print(f"Area buildings in the final cluster: {cluster_area_building_count} "
      f"({round(10000.0 * cluster_area_building_count / cluster_building_count) / 100.0}% of all buildings in the final cluster)")
print("===========================================================================================")
