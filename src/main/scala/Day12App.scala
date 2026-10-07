import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day12App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 12 - Cache and Persist")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val sc = spark.sparkContext

    println("===== DAY 12: CACHE AND PERSIST =====")

    val transactions = sc.parallelize(
      Seq(
        ("T001", "Amit", "Food", 1200),
        ("T002", "Rahul", "Electronics", 45000),
        ("T003", "Sara", "Food", 2500),
        ("T004", "Amit", "Electronics", 65000),
        ("T005", "Neha", "Clothing", 5000),
        ("T006", "Rahul", "Food", 1800),
        ("T007", "Sara", "Electronics", 35000)
      )
    )

    val cleanedTransactions = transactions
      .filter(_._4 > 1000)
      .map {
        case (id, customer, category, amount) =>
          (id, customer, category, amount)
      }

    println("\nWITHOUT CACHE:")
    println(s"Count: ${cleanedTransactions.count()}")
    println(s"High Value: ${cleanedTransactions.filter(_._4 > 30000).count()}")

    println("\nUSING CACHE:")

    val cachedTransactions = cleanedTransactions.cache()

    println(s"Total records: ${cachedTransactions.count()}")

    println(
      s"Electronics records: ${
        cachedTransactions.filter(_._3 == "Electronics").count()
      }"
    )

    println(
      s"High value records: ${
        cachedTransactions.filter(_._4 > 30000).count()
      }"
    )

    println("\nUSING PERSIST:")

    val persistedTransactions =
      cleanedTransactions.persist(StorageLevel.MEMORY_ONLY)

    println(s"Persisted count: ${persistedTransactions.count()}")

    println(
      s"Persisted high-value count: ${
        persistedTransactions.filter(_._4 > 30000).count()
      }"
    )

    println("\nSTORAGE LEVEL:")
    println(StorageLevel.MEMORY_ONLY)

    cachedTransactions.unpersist()
    persistedTransactions.unpersist()

    println("\nCACHE VS PERSIST:")
    println("cache() stores data using the default storage level.")
    println("persist() allows choosing a specific storage level.")

    println("\nWHEN CACHE CAN HURT:")
    println("Caching can hurt when the dataset is very large, used only once,")
    println("or consumes too much executor memory.")

    spark.stop()
  }
}
